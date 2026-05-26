// Package api реализует HTTP-сервер сервиса trending.
//
// Маршруты:
//
//	GET  /api/v1/top                      — топ поисковых запросов (hot path)
//	GET  /api/v1/admin/stoplist           — просмотр стоп-листа
//	POST /api/v1/admin/stoplist           — добавить слово в стоп-лист
//	DELETE /api/v1/admin/stoplist/{word}  — удалить слово из стоп-листа
//	GET  /health                          — liveness probe
//	GET  /metrics                         — Prometheus
//
// Go 1.22+ ServeMux поддерживает "METHOD /path" паттерны и path-параметры {name}.
// Go 1.23+ r.Pattern отдаёт совпавший паттерн для метрик (низкая кардинальность).
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/artyomstank/RWB_intern/internal/domain"
	"github.com/artyomstank/RWB_intern/internal/metrics"
	"github.com/artyomstank/RWB_intern/internal/stoplist"
	"github.com/artyomstank/RWB_intern/internal/topn"
)

// NewMux собирает http.Handler с маршрутами и middleware.
func NewMux(
	cache *topn.Cache,
	sl *stoplist.StopList,
	m *metrics.Metrics,
	promHandler http.Handler,
) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/top", handleTop(cache))
	mux.HandleFunc("GET /api/v1/admin/stoplist", handleListStoplist(sl))
	mux.HandleFunc("POST /api/v1/admin/stoplist", handleAddStoplist(sl, m))
	mux.HandleFunc("DELETE /api/v1/admin/stoplist/{word}", handleDeleteStoplist(sl, m))
	mux.HandleFunc("GET /health", handleHealth)
	mux.Handle("GET /metrics", promHandler)

	return metricsMiddleware(m)(mux)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// handleTop — горячий путь. atomic.Load + write precomputed bytes.
// Никаких аллокаций в типичном случае.
func handleTop(cache *topn.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := cache.Load()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")

		if result == nil {
			// Холодный старт: топ ещё не вычислен.
			w.Write([]byte(`{"queries":[],"window":"5m","total_active":0,"updated_at":null}`))
			return
		}

		// Опциональный параметр ?limit=N для мобильных клиентов.
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 && limit < len(result.Items) {
				writeTopSubset(w, result, limit)
				return
			}
		}

		// Типичный случай: пишем предсериализованные байты без аллокаций.
		w.Write(result.Serialized)
	}
}

// writeTopSubset сериализует срез результата для ?limit=N запросов.
// Аллоцирует — но вызывается редко (нестандартный limit).
func writeTopSubset(w http.ResponseWriter, result *topn.CachedResult, limit int) {
	subset := result.Items[:limit]
	resp := topn.TopResponse{
		Queries:     subset,
		Window:      "5m",
		UpdatedAt:   result.UpdatedAt,
		TotalActive: result.TotalActive,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("api: failed to encode top subset", "err", err)
	}
}

// handleListStoplist возвращает все слова стоп-листа.
func handleListStoplist(sl *stoplist.StopList) http.HandlerFunc {
	type response struct {
		Words []stoplist.Entry `json:"words"`
		Total int              `json:"total"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		entries := sl.List()
		writeJSON(w, http.StatusOK, response{
			Words: entries,
			Total: len(entries),
		})
	}
}

// handleAddStoplist добавляет слово в стоп-лист.
func handleAddStoplist(sl *stoplist.StopList, m *metrics.Metrics) http.HandlerFunc {
	type request struct {
		Word string `json:"word"`
	}
	type response struct {
		Status string `json:"status"`
		Word   string `json:"word"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Word == "" {
			writeError(w, http.StatusBadRequest, "field 'word' is required and must be a non-empty string")
			return
		}

		if err := sl.Add(req.Word); err != nil {
			slog.Error("api: stoplist add failed", "word", req.Word, "err", err)
			writeError(w, http.StatusInternalServerError, "failed to add word to stoplist")
			return
		}

		m.StopListSize.Set(float64(len(sl.List())))
		writeJSON(w, http.StatusOK, response{Status: "added", Word: req.Word})
	}
}

// handleDeleteStoplist удаляет слово из стоп-листа.
// Использует path-параметр {word} — Go 1.22+ ServeMux.
func handleDeleteStoplist(sl *stoplist.StopList, m *metrics.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		word := r.PathValue("word")
		if word == "" {
			writeError(w, http.StatusBadRequest, "word path parameter is required")
			return
		}

		if err := sl.Remove(word); err != nil {
			slog.Error("api: stoplist remove failed", "word", word, "err", err)
			writeError(w, http.StatusInternalServerError, "failed to remove word from stoplist")
			return
		}

		m.StopListSize.Set(float64(len(sl.List())))
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleHealth — liveness probe для Docker/k8s healthcheck.
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

// ── Middleware ─────────────────────────────────────────────────────────────────

// responseWriter перехватывает статус-код для метрик.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.written = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.written = true
	}
	return rw.ResponseWriter.Write(b)
}

// metricsMiddleware измеряет латентность HTTP-запросов.
// r.Pattern (Go 1.23+) — совпавший маршрутный паттерн, низкая кардинальность.
func metricsMiddleware(m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rw, r)

			pattern := r.Pattern
			if pattern == "" {
				pattern = "unmatched"
			}
			m.HTTPDuration.WithLabelValues(
				r.Method,
				pattern,
				strconv.Itoa(rw.statusCode),
			).Observe(time.Since(start).Seconds())
		})
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("api: failed to encode response", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// Ensure domain is used (TopEntry is referenced via TopResponse in topn package).
var _ domain.TopEntry
