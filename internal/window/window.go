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

// Stats — агрегированная статистика одного запроса за скользящее окно.
type Stats struct {
	Count       int64 // Changed from int32 to prevent overflow at high concurrency
	UniqueUsers uint64
}

// secBucket — один секундный слот кольцевого буфера.
type secBucket struct {
	second int64            // unix timestamp этого слота (0 = пустой)
	counts map[uint64]int64 // hash(query) → количество событий (int64 to prevent overflow)
}

// minBucket — один минутный слот кольцевого буфера.
type minBucket struct {
	minute int64                          // unix/60
	hlls   map[uint64]*hyperloglog.Sketch // hash(query) → HLL(userIDs)
}

// shard — независимая единица хранения с собственным мьютексом.
type shard struct {
	mu         sync.Mutex
	secBuckets [secWindow]secBucket
	secHead    int // текущий (самый новый) слот
	minBuckets [minWindow]minBucket
	minHead    int
}

// registry отображает hash → нормализованная строка запроса.
// Все горячие структуры работают с uint64 хэшами; строка хранится ровно один раз.
type registry struct {
	mu   sync.RWMutex
	data map[uint64]string
}

func (r *registry) put(h uint64, s string) {
	// Double-checked locking: большинство запросов уже в реестре.
	r.mu.RLock()
	_, ok := r.data[h]
	r.mu.RUnlock()
	if ok {
		return
	}
	r.mu.Lock()
	r.data[h] = s // идемпотентно при race
	r.mu.Unlock()
}

func (r *registry) lookup(h uint64) (string, bool) {
	r.mu.RLock()
	s, ok := r.data[h]
	r.mu.RUnlock()
	return s, ok
}

// prune удаляет хэши, которых больше нет в скользящем окне.
// maps.DeleteFunc — Go 1.21+.
func (r *registry) prune(active map[uint64]struct{}) {
	r.mu.Lock()
	maps.DeleteFunc(r.data, func(k uint64, _ string) bool {
		_, ok := active[k]
		return !ok
	})
	r.mu.Unlock()
}

// Window — основная структура скользящего окна.
type Window struct {
	shards [numShards]shard
	reg    registry
}

// New инициализирует Window с готовыми внутренними картами.
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

// Record добавляет событие поиска в скользящее окно.
// Возвращает false если событие отфильтровано (late/future arrival или пустой запрос).
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

	// Секундный бакет — используем event time.
	sec := event.Ts.Unix()
	rotateSec(sh, sec)
	sh.secBuckets[sh.secHead].counts[h]++

	// Минутный HLL-бакет — для подсчёта уникальных пользователей.
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

// LookupQuery возвращает нормализованную строку запроса по хэшу.
func (w *Window) LookupQuery(h uint64) (string, bool) {
	return w.reg.lookup(h)
}

// Snapshot вычисляет агрегированную статистику для всех запросов в окне.
// Вызывается фоновым воркером раз в N секунд — не в hot path чтения.
func (w *Window) Snapshot() map[uint64]Stats {
	now := time.Now()
	secCutoff := now.Unix() - secWindow               // секунды старше этого — вне окна
	minCutoff := now.Unix()/60 - int64(minWindow) + 1 // минуты в окне

	result := make(map[uint64]Stats)
	active := make(map[uint64]struct{})
	// Аккумулятор HLL по всем шардам. Создаём снаружи lock'а.
	hllAcc := make(map[uint64]*hyperloglog.Sketch)

	for i := range w.shards {
		sh := &w.shards[i]
		sh.mu.Lock()

		// Агрегируем счётчики из секундных бакетов.
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

		// Мержим HLL из минутных бакетов. Пересечение с shardCounts:
		// если запрос вышел из секундного окна — HLL нам не нужен.
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
					_ = agg.Merge(sketch) // Merge под локом шарда — race-free
				} else {
					// Создаём новый скетч и мержим — не мутируем хранимый.
					fresh := hyperloglog.New14()
					_ = fresh.Merge(sketch)
					hllAcc[h] = fresh
				}
			}
		}

		sh.mu.Unlock()

		// Аккумулируем в глобальный результат (single goroutine, нет race).
		for h, c := range shardCounts {
			active[h] = struct{}{}
			e := result[h]
			e.Count += c
			result[h] = e
		}
	}

	// Заполняем UniqueUsers из HLL-оценок.
	for h, hll := range hllAcc {
		if e, ok := result[h]; ok {
			e.UniqueUsers = hll.Estimate()
			result[h] = e
		}
	}

	// Вычищаем Registry от хэшей, которых больше нет в окне.
	w.reg.prune(active)

	return result
}

// rotateSec сдвигает голову кольца секундных бакетов до нужной секунды.
// Вызывается под sh.mu.Lock().
func rotateSec(sh *shard, sec int64) {
	cur := &sh.secBuckets[sh.secHead]
	if cur.second == sec {
		return
	}
	if cur.second == 0 {
		// Первая запись в шарде.
		cur.second = sec
		return
	}
	if sec <= cur.second {
		return // out-of-order событие — записываем в текущий бакет выше
	}

	diff := sec - cur.second
	if diff > secWindow {
		diff = secWindow // при большом пропуске очищаем всё окно
	}

	// for range N — Go 1.22+
	for range diff {
		sh.secHead = (sh.secHead + 1) % secWindow
		b := &sh.secBuckets[sh.secHead]
		clear(b.counts) // clear(map) — Go 1.21+; переиспользует внутренние бакеты
		b.second = 0
	}
	sh.secBuckets[sh.secHead].second = sec
}

// rotateMin сдвигает голову кольца минутных бакетов до нужной минуты.
// Вызывается под sh.mu.Lock().
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
		clear(b.hlls) // GC соберёт старые HLL-скетчи
		b.minute = 0
	}
	sh.minBuckets[sh.minHead].minute = min
}

// globalSeed фиксируется один раз при инициализации пакета.
// maphash быстрее FNV-1a и не аллоцирует — важно при 100k событий/сек.
var globalSeed = maphash.MakeSeed()

// hashQuery возвращает 64-битный хэш нормализованного запроса.
// Использует maphash для zero-allocation хеширования в hot path.
func hashQuery(q string) uint64 {
	return maphash.String(globalSeed, q)
}

// normalize приводит запрос к нормализованной форме:
// NFC → lowercase → trim → collapse spaces.
func normalize(s string) string {
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}
