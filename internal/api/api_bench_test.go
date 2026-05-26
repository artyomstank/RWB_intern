package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/artyomstank/RWB_intern/internal/domain"
	"github.com/artyomstank/RWB_intern/internal/metrics"
	"github.com/artyomstank/RWB_intern/internal/stoplist"
	"github.com/artyomstank/RWB_intern/internal/topn"
)

// setupTestHarness создаёт готовый HTTP handler для бенчмарков.
func setupTestHarness() (http.Handler, *topn.Cache) {
	// Создаём временный файл для stoplist
	tmpFile, err := os.CreateTemp("", "stoplist_bench_*.db")
	if err != nil {
		tmpFile = nil
	} else {
		tmpFile.Close()
		defer os.Remove(tmpFile.Name())
	}

	var sl *stoplist.StopList
	if tmpFile != nil {
		sl, _ = stoplist.New(tmpFile.Name())
	} else {
		sl, _ = stoplist.New(":memory:")
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	// Создаём cache с фиксированным результатом
	cache := &topn.Cache{}

	// Заполняем cache тестовыми данными
	items := make([]domain.TopEntry, 100)
	for i := 0; i < 100; i++ {
		items[i] = domain.TopEntry{
			Rank:        i + 1,
			Query:       fmt.Sprintf("query_%d", i),
			Count:       int32(100 - i),
			UniqueUsers: uint64(50 + i%50),
		}
	}

	result := &topn.CachedResult{
		Items:       items,
		TotalActive: 5000,
		UpdatedAt:   time.Now(),
	}

	// Сохраняем сериализованные байты
	data, _ := json.Marshal(topn.TopResponse{
		Queries:     result.Items,
		Window:      "5m",
		UpdatedAt:   result.UpdatedAt,
		TotalActive: result.TotalActive,
	})
	result.Serialized = data

	cache.val.Store(result)

	// Создаём prometheus handler
	promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	handler := NewMux(cache, sl, m, promHandler)
	return handler, cache
}

// setupCacheWithSize создаёт кэш с произвольным количеством элементов
func setupCacheWithSize(size int) *topn.Cache {
	cache := &topn.Cache{}

	items := make([]domain.TopEntry, size)
	for i := 0; i < size; i++ {
		items[i] = domain.TopEntry{
			Rank:        i + 1,
			Query:       fmt.Sprintf("query_%d", i),
			Count:       int32(size - i),
			UniqueUsers: uint64(50),
		}
	}

	result := &topn.CachedResult{
		Items:       items,
		TotalActive: size * 50,
		UpdatedAt:   time.Now(),
	}

	data, _ := json.Marshal(topn.TopResponse{
		Queries:     result.Items,
		Window:      "5m",
		UpdatedAt:   result.UpdatedAt,
		TotalActive: result.TotalActive,
	})
	result.Serialized = data

	cache.val.Store(result)
	return cache
}

// BenchmarkTopEndpoint — быстрая проверка производительности GET /api/v1/top
func BenchmarkTopEndpoint(b *testing.B) {
	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
		},
	}
	defer client.CloseIdleConnections()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(server.URL + "/api/v1/top")
			if err != nil {
				b.Fatalf("request failed: %v", err)
			}
			io.ReadAll(resp.Body)
			resp.Body.Close()
		}
	})
}

// BenchmarkTopEndpointWithLimit — GET /api/v1/top?limit=10
func BenchmarkTopEndpointWithLimit(b *testing.B) {
	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}
	defer client.CloseIdleConnections()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(server.URL + "/api/v1/top?limit=10")
			if err != nil {
				b.Fatalf("request failed: %v", err)
			}
			io.ReadAll(resp.Body)
			resp.Body.Close()
		}
	})
}

// BenchmarkHealthEndpoint — проверка /health endpoint
func BenchmarkHealthEndpoint(b *testing.B) {
	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	defer client.CloseIdleConnections()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(server.URL + "/health")
			if err != nil {
				b.Fatalf("request failed: %v", err)
			}
			io.ReadAll(resp.Body)
			resp.Body.Close()
		}
	})
}

// BenchmarkStoplistRead — тестирование операций чтения со стоп-листом
func BenchmarkStoplistRead(b *testing.B) {
	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	defer client.CloseIdleConnections()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, _ := client.Get(server.URL + "/api/v1/admin/stoplist")
			if resp != nil {
				io.ReadAll(resp.Body)
				resp.Body.Close()
			}
		}
	})
}

// BenchmarkConcurrentRequests — нагрузка с разными уровнями параллелизма
func BenchmarkConcurrentRequests(b *testing.B) {
	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	concurrencyLevels := []int{1, 10, 50, 100}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			client := &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					MaxIdleConns:        concurrency,
					MaxIdleConnsPerHost: concurrency,
					MaxConnsPerHost:     concurrency,
				},
			}
			defer client.CloseIdleConnections()

			b.ResetTimer()

			var wg sync.WaitGroup
			limiter := make(chan struct{}, concurrency)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				limiter <- struct{}{}

				go func() {
					defer wg.Done()
					defer func() { <-limiter }()

					resp, _ := client.Get(server.URL + "/api/v1/top")
					if resp != nil {
						io.ReadAll(resp.Body)
						resp.Body.Close()
					}
				}()
			}

			wg.Wait()
		})
	}
}

// BenchmarkResponsePayload — измерение нагрузки при разных размерах ответов
func BenchmarkResponsePayload(b *testing.B) {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}
	defer client.CloseIdleConnections()

	payloadSizes := []int{10, 50, 100, 500, 1000}

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("payload_size=%d", size), func(b *testing.B) {
			cache := setupCacheWithSize(size)

			tmpFile, _ := os.CreateTemp("", "stoplist_bench_*.db")
			if tmpFile != nil {
				tmpFile.Close()
				defer os.Remove(tmpFile.Name())
			}

			sl, _ := stoplist.New(tmpFile.Name())
			defer sl.Close()

			reg := prometheus.NewRegistry()
			m := metrics.New(reg)
			promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

			handler := NewMux(cache, sl, m, promHandler)
			server := httptest.NewServer(handler)
			defer server.Close()

			result := cache.Load()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					resp, _ := client.Get(server.URL + "/api/v1/top")
					io.ReadAll(resp.Body)
					resp.Body.Close()
				}
			})

			if result != nil {
				b.ReportMetric(float64(len(result.Serialized)), "response_bytes")
			}
		})
	}
}

// StressTest — долгосрочный стресс-тест с мониторингом
func TestStressLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			MaxConnsPerHost:     200,
		},
	}
	defer client.CloseIdleConnections()

	concurrency := 100
	duration := 30 * time.Second
	successCount := 0
	errorCount := 0
	var mu sync.Mutex

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	start := time.Now()
	var wg sync.WaitGroup
	limiter := make(chan struct{}, concurrency)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			elapsed := time.Since(start)
			t.Logf("\n=== Stress Test Results ===")
			t.Logf("Duration: %v", elapsed)
			t.Logf("Concurrency: %d", concurrency)
			t.Logf("Successful requests: %d", successCount)
			t.Logf("Failed requests: %d", errorCount)
			t.Logf("Requests/sec: %.2f", float64(successCount)/elapsed.Seconds())
			return
		default:
			wg.Add(1)
			limiter <- struct{}{}

			go func() {
				defer wg.Done()
				defer func() { <-limiter }()

				resp, err := client.Get(server.URL + "/api/v1/top")
				mu.Lock()
				if err != nil {
					errorCount++
				} else {
					successCount++
					io.ReadAll(resp.Body)
					resp.Body.Close()
				}
				mu.Unlock()
			}()
		}
	}
}

// LoadTest — загрузочный тест с метриками задержки
func TestLoadMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load metrics test in short mode")
	}

	handler, _ := setupTestHarness()
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 50,
		},
	}
	defer client.CloseIdleConnections()

	var latencies []time.Duration
	var mu sync.Mutex

	concurrency := 50
	totalRequests := 5000

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency)

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()

			start := time.Now()
			resp, err := client.Get(server.URL + "/api/v1/top")
			latency := time.Since(start)

			mu.Lock()
			latencies = append(latencies, latency)
			mu.Unlock()

			if err == nil {
				io.ReadAll(resp.Body)
				resp.Body.Close()
			}
		}()
	}

	wg.Wait()

	// Вычисляем статистику
	var sum time.Duration
	min := latencies[0]
	max := latencies[0]

	for _, lat := range latencies {
		sum += lat
		if lat < min {
			min = lat
		}
		if lat > max {
			max = lat
		}
	}

	avg := sum / time.Duration(len(latencies))

	// Сортируем для перцентилей
	// Простой способ для демонстрации
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]

	t.Logf("\n=== Load Test Metrics ===")
	t.Logf("Total Requests: %d", totalRequests)
	t.Logf("Concurrency: %d", concurrency)
	t.Logf("Min Latency: %v", min)
	t.Logf("Max Latency: %v", max)
	t.Logf("Avg Latency: %v", avg)
	t.Logf("P95 Latency: %v", p95)
	t.Logf("P99 Latency: %v", p99)
	t.Logf("Requests/sec: %.2f", float64(totalRequests)/(sum.Seconds()))
}
