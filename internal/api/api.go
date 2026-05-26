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

func handleTop(cache *topn.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := cache.Load()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")

		if result == nil {
			w.Write([]byte(`{"queries":[],"window":"5m","total_active":0,"updated_at":null}`))
			return
		}

		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 && limit < len(result.Items) {
				writeTopSubset(w, result, limit)
				return
			}
		}

		w.Write(result.Serialized)
	}
}

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

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

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

var _ domain.TopEntry
