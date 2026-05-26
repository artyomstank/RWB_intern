// Package topn реализует кэш результатов и фоновый воркер пересчёта топа.
//
// Архитектура чтения:
//
//	Воркер раз в RefreshTTL вычисляет топ, сериализует JSON и атомарно
//	заменяет указатель на CachedResult. Хендлер GET /top делает Load()
//	и пишет Serialized в ответ — никаких мьютексов в критическом пути.
//
//	atomic.Pointer[T] (Go 1.19+) хранит один указатель без boxing,
//	в отличие от atomic.Value — это критично при 50k rps.
package topn

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"cmp"

	"github.com/artyomstank/RWB_intern/internal/anomaly"
	"github.com/artyomstank/RWB_intern/internal/domain"
	"github.com/artyomstank/RWB_intern/internal/metrics"
	"github.com/artyomstank/RWB_intern/internal/stoplist"
	"github.com/artyomstank/RWB_intern/internal/window"
)

// TopResponse — структура JSON-ответа GET /api/v1/top.
// Определена здесь, а не в api, так как воркер сериализует её при каждом обновлении.
type TopResponse struct {
	Queries     []domain.TopEntry `json:"queries"`
	Window      string            `json:"window"`
	UpdatedAt   time.Time         `json:"updated_at"`
	TotalActive int               `json:"total_active"`
}

// CachedResult — неизменяемый снапшот топа.
// Хранится по указателю; атомарная замена при каждом обновлении.
type CachedResult struct {
	Items       []domain.TopEntry
	UpdatedAt   time.Time
	TotalActive int
	// Serialized — предсериализованный JSON полного TopResponse.
	// Хендлер пишет эти байты напрямую в ResponseWriter без дополнительных аллокаций.
	Serialized []byte
}

// Cache — lock-free кэш последнего результата.
type Cache struct {
	val atomic.Pointer[CachedResult]
}

// Load возвращает актуальный кэш. Может вернуть nil до первого вычисления.
func (c *Cache) Load() *CachedResult {
	return c.val.Load()
}

// store атомарно обновляет кэш.
func (c *Cache) store(r *CachedResult) {
	c.val.Store(r)
}

// Worker — фоновый воркер, пересчитывающий топ каждые RefreshTTL.
type Worker struct {
	win      *window.Window
	sl       *stoplist.StopList
	detector *anomaly.Detector
	cache    *Cache
	m        *metrics.Metrics
	n        int
	refresh  time.Duration
}

// NewWorker создаёт Worker.
func NewWorker(
	win *window.Window,
	sl *stoplist.StopList,
	detector *anomaly.Detector,
	cache *Cache,
	m *metrics.Metrics,
	n int,
	refresh time.Duration,
) *Worker {
	return &Worker{
		win:      win,
		sl:       sl,
		detector: detector,
		cache:    cache,
		m:        m,
		n:        n,
		refresh:  refresh,
	}
}

// Run запускает воркер. Блокирует до отмены ctx.
func (w *Worker) Run(ctx context.Context) {
	// Первый вычисление сразу при старте — чтобы не ждать первый тик.
	w.compute()

	ticker := time.NewTicker(w.refresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.compute()
		}
	}
}

// compute — единственная горячая логика воркера.
// Выполняется в одном goroutine, нет конкурентного доступа к compute().
func (w *Worker) compute() {
	start := time.Now()

	snapshot := w.win.Snapshot()

	type candidate struct {
		hash        uint64
		query       string
		count       int64 // Changed from int32 to match window.Stats.Count
		uniqueUsers uint64
	}

	candidates := make([]candidate, 0, len(snapshot))

	for h, stats := range snapshot {
		q, ok := w.win.LookupQuery(h)
		if !ok {
			continue // был вычищен из Registry (race между Snapshot и Prune) — редко
		}

		// Фильтр 1: стоп-лист (точное совпадение нормализованного запроса).
		if w.sl.Contains(q) {
			continue
		}

		// Фильтр 2: двухуровневый детектор аномалий.
		if w.detector.IsAnomaly(h, stats.Count, stats.UniqueUsers) {
			w.m.AnomaliesDetected.Inc()
			slog.Debug("anomaly filtered", "query", q, "count", stats.Count, "unique_users", stats.UniqueUsers)
			continue
		}

		candidates = append(candidates, candidate{
			hash:        h,
			query:       q,
			count:       stats.Count,
			uniqueUsers: stats.UniqueUsers,
		})
	}

	// Собираем active hashes из snapshot для pruning детектора.
	active := make(map[uint64]struct{}, len(snapshot))
	for h := range snapshot {
		active[h] = struct{}{}
	}
	w.detector.Prune(active)

	// Сортируем по count desc, при равенстве — по query asc (стабильность).
	slices.SortFunc(candidates, func(a, b candidate) int {
		if c := cmp.Compare(b.count, a.count); c != 0 {
			return c
		}
		return cmp.Compare(a.query, b.query)
	})

	if len(candidates) > w.n {
		candidates = candidates[:w.n]
	}

	items := make([]domain.TopEntry, len(candidates))
	for i, c := range candidates {
		items[i] = domain.TopEntry{
			Rank:        i + 1,
			Query:       c.query,
			Count:       int32(c.count),
			UniqueUsers: c.uniqueUsers,
		}
	}

	updatedAt := time.Now().UTC()
	resp := TopResponse{
		Queries:     items,
		Window:      "5m",
		UpdatedAt:   updatedAt,
		TotalActive: len(snapshot),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		slog.Error("topn: failed to marshal result", "err", err)
		return
	}

	w.cache.store(&CachedResult{
		Items:       items,
		UpdatedAt:   updatedAt,
		TotalActive: len(snapshot),
		Serialized:  data,
	})

	elapsed := time.Since(start)
	w.m.TopNComputeTime.Observe(elapsed.Seconds())
	w.m.TopNLastUpdated.Set(float64(time.Now().Unix()))

	slog.Debug("topn computed",
		"candidates", len(snapshot),
		"top", len(items),
		"elapsed_ms", elapsed.Milliseconds(),
	)
}
