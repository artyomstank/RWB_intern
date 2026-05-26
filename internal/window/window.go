// Package window реализует скользящее окно для подсчёта поисковых событий.
//
// Архитектура хранения:
//
//	64 шарда (hash(query) & 63) — устраняет конкуренцию за мьютекс.
//	Каждый шард содержит:
//	  - [300]secBucket  — кольцевой буфер секундных агрегатов (5 минут).
//	  - [6]minBucket    — кольцевой буфер минутных HLL-скетчей (anti-gaming).
//	Registry            — единственное место хранения строк (hash→string).
//
// Горячий путь (Record): O(1), один мьютекс на шард.
// Снапшот (Snapshot): O(shards × buckets × queries/shard) ≈ O(N).
package window

import (
	"hash/maphash"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"golang.org/x/text/unicode/norm"

	"github.com/artyomstank/RWB_intern/internal/domain"
)

const (
	numShards   = 64  // степень двойки: & 63 вместо % 64
	secWindow   = 300 // 5 минут в секундах
	minWindow   = 6   // 6 минутных бакетов (5 активных + 1 rotate-буфер)
	lateLimit   = 60 * time.Second
	futureLimit = 10 * time.Second
)

type Stats struct {
	Count       int64
	UniqueUsers uint64
}

type secBucket struct {
	second int64
	counts map[uint64]int64
}

type minBucket struct {
	minute int64
	hlls   map[uint64]*hyperloglog.Sketch
}

type shard struct {
	mu         sync.Mutex
	secBuckets [secWindow]secBucket
	secHead    int
	minBuckets [minWindow]minBucket
	minHead    int
}

type registry struct {
	mu   sync.RWMutex
	data map[uint64]string
}

func (r *registry) put(h uint64, s string) {
	r.mu.RLock()
	_, ok := r.data[h]
	r.mu.RUnlock()
	if ok {
		return
	}
	r.mu.Lock()
	r.data[h] = s
	r.mu.Unlock()
}

func (r *registry) lookup(h uint64) (string, bool) {
	r.mu.RLock()
	s, ok := r.data[h]
	r.mu.RUnlock()
	return s, ok
}

func (r *registry) prune(active map[uint64]struct{}) {
	r.mu.Lock()
	maps.DeleteFunc(r.data, func(k uint64, _ string) bool {
		_, ok := active[k]
		return !ok
	})
	r.mu.Unlock()
}

type Window struct {
	shards [numShards]shard
	reg    registry
}

func New() *Window {
	w := &Window{}
	w.reg.data = make(map[uint64]string)
	for i := range w.shards {
		for j := range w.shards[i].secBuckets {
			w.shards[i].secBuckets[j].counts = make(map[uint64]int64)
		}
		for j := range w.shards[i].minBuckets {
			w.shards[i].minBuckets[j].hlls = make(map[uint64]*hyperloglog.Sketch)
		}
	}
	return w
}

func (w *Window) Record(event *domain.SearchEvent) bool {
	now := time.Now()
	age := now.Sub(event.Ts)
	if age > lateLimit || age < -futureLimit {
		return false
	}

	q := normalize(event.Query)
	if q == "" {
		return false
	}

	h := hashQuery(q)
	w.reg.put(h, q)

	sid := h & (numShards - 1)
	sh := &w.shards[sid]

	sh.mu.Lock()

	sec := event.Ts.Unix()
	rotateSec(sh, sec)
	sh.secBuckets[sh.secHead].counts[h]++

	min := event.Ts.Unix() / 60
	rotateMin(sh, min)
	mb := &sh.minBuckets[sh.minHead]
	sketch := mb.hlls[h]
	if sketch == nil {
		sketch = hyperloglog.New14()
		mb.hlls[h] = sketch
	}
	sketch.Insert([]byte(event.UserID))

	sh.mu.Unlock()
	return true
}

func (w *Window) LookupQuery(h uint64) (string, bool) {
	return w.reg.lookup(h)
}

func (w *Window) Snapshot() map[uint64]Stats {
	now := time.Now()
	secCutoff := now.Unix() - secWindow
	minCutoff := now.Unix()/60 - int64(minWindow) + 1
	result := make(map[uint64]Stats)
	active := make(map[uint64]struct{})
	hllAcc := make(map[uint64]*hyperloglog.Sketch)

	for i := range w.shards {
		sh := &w.shards[i]
		sh.mu.Lock()

		shardCounts := make(map[uint64]int64)
		for j := range sh.secBuckets {
			b := &sh.secBuckets[j]
			if b.second == 0 || b.second <= secCutoff {
				continue
			}
			for h, c := range b.counts {
				shardCounts[h] += c
			}
		}

		for j := range sh.minBuckets {
			b := &sh.minBuckets[j]
			if b.minute == 0 || b.minute < minCutoff {
				continue
			}
			for h, sketch := range b.hlls {
				if _, ok := shardCounts[h]; !ok {
					continue
				}
				if agg, ok := hllAcc[h]; ok {
					_ = agg.Merge(sketch)
				} else {
					fresh := hyperloglog.New14()
					_ = fresh.Merge(sketch)
					hllAcc[h] = fresh
				}
			}
		}

		sh.mu.Unlock()

		for h, c := range shardCounts {
			active[h] = struct{}{}
			e := result[h]
			e.Count += c
			result[h] = e
		}
	}

	for h, hll := range hllAcc {
		if e, ok := result[h]; ok {
			e.UniqueUsers = hll.Estimate()
			result[h] = e
		}
	}

	w.reg.prune(active)

	return result
}

// rotateSec сдвигает голову кольца секундных бакетов до нужной секунды.
func rotateSec(sh *shard, sec int64) {
	cur := &sh.secBuckets[sh.secHead]
	if cur.second == sec {
		return
	}
	if cur.second == 0 {
		cur.second = sec
		return
	}
	if sec <= cur.second {
		return
	}

	diff := sec - cur.second
	if diff > secWindow {
		diff = secWindow
	}

	for range diff {
		sh.secHead = (sh.secHead + 1) % secWindow
		b := &sh.secBuckets[sh.secHead]
		clear(b.counts)
		b.second = 0
	}
	sh.secBuckets[sh.secHead].second = sec
}

func rotateMin(sh *shard, min int64) {
	cur := &sh.minBuckets[sh.minHead]
	if cur.minute == min {
		return
	}
	if cur.minute == 0 {
		cur.minute = min
		return
	}
	if min <= cur.minute {
		return
	}

	diff := min - cur.minute
	if diff > minWindow {
		diff = minWindow
	}

	for range diff {
		sh.minHead = (sh.minHead + 1) % minWindow
		b := &sh.minBuckets[sh.minHead]
		clear(b.hlls)
		b.minute = 0
	}
	sh.minBuckets[sh.minHead].minute = min
}

var globalSeed = maphash.MakeSeed()

func hashQuery(q string) uint64 {
	return maphash.String(globalSeed, q)
}

func normalize(s string) string {
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}
