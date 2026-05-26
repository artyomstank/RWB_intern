# Быстрый старт нагрузочного тестирования

## 🚀 За 5 минут

### 1. Go Benchmarks (встроенные)

```bash
cd /path/to/RWB_intern

# Запуск всех бенчмарков
go test -bench=. -benchmem -benchtime=5s ./internal/api
```

**Ожидаемый результат:**
```
BenchmarkTopEndpoint-8           500000   2480 ns/op   0 B/op   0 allocs/op
BenchmarkConcurrentRequests/concurrency=1-8        100000  10200 ns/op
BenchmarkConcurrentRequests/concurrency=10-8       500000   2500 ns/op
```

### 2. Стресс-тест (долгосрочный)

```bash
# В одном терминале - запуск приложения
go run ./cmd/main.go

# В другом терминале - нагрузка
go test -run=TestStressLoad -v -timeout=60s ./internal/api
```

### 3. Hey нагрузочное тестирование

```bash
# Установка
go install github.com/rakyll/hey@latest

# Нагрузка: 60 секунд, 100 параллельных соединений
hey -z 60s -c 100 http://localhost:8080/api/v1/top
```

## 📊 Интерпретация результатов

### Go Benchmark формат

```
BenchmarkTopEndpoint-8    500000    2480 ns/op    0 B/op    0 allocs/op
                                    └─────┬──────┘
                          Наносекунды на операцию = ~404k операций/сек
```

| Метрика | Значение | Оценка |
|---------|----------|--------|
| ns/op (наносекунды) | < 5000 | ✓ Отлично |
| B/op (байты) | 0 | ✓ Отлично |
| allocs/op | 0 | ✓ Отлично |

### Hey формат

```
Requests/sec:     8321.10
Latencies         Avg: 12.34 ms, Min: 0.21 ms, Max: 45 ms
Status code 200:  498000 requests
```

| Метрика | Значение | Оценка |
|---------|----------|--------|
| Requests/sec | > 10000 | ✓ Хорошо |
| Latency (avg) | < 50ms | ✓ Хорошо |
| Latency (max) | < 100ms | ✓ Хорошо |
| Error rate | 0% | ✓ Отлично |

## 📁 Структура файлов

```
/path/to/RWB_intern/
├── internal/api/
│   └── api_bench_test.go          ← Go benchmarks & stress tests
├── bench_load_test.sh              ← Комбинированные тесты
├── load_test_hey.sh                ← Hey нагрузочное тестирование
├── load_test_vegeta.sh             ← Vegeta нагрузочное тестирование
├── load_test_wrk.sh                ← WRK нагрузочное тестирование
├── LOAD_TESTING_GUIDE.md           ← Подробное руководство
└── benchmark_results/              ← Результаты (автоматически создается)
    ├── 01_benchmark_top_endpoint.txt
    ├── 05_stress_test.txt
    ├── 07_hey_results.txt
    └── SUMMARY.md
```

## 🔧 Установка инструментов

### Hey

```bash
go install github.com/rakyll/hey@latest
```

### Vegeta

```bash
go install github.com/tsenart/vegeta@latest
```

### WRK

```bash
# macOS
brew install wrk

# Ubuntu
sudo apt-get install wrk

# Или скомпилировать с исходников
git clone https://github.com/wg/wrk.git
cd wrk && make
```

## 📈 Типичные результаты

### Hot Path (`GET /api/v1/top`)

```
Hey (100 concurrent):
  Requests/sec: 15,000-25,000
  Latency (p50): < 2ms
  Latency (p99): < 10ms
  Error rate: 0%

Go Benchmark:
  Operations/sec: 400,000+
  Allocations/op: 0
```

### Health Check (`GET /health`)

```
WRK (100 concurrent):
  Requests/sec: 30,000-50,000
  Latency (p99): < 5ms
```

## 🎯 Полный цикл тестирования (~30 минут)

```bash
#!/bin/bash

# 1. Go benchmarks (~2 минуты)
go test -bench=. -benchmem -benchtime=5s ./internal/api

# 2. Запуск приложения фоном
go run ./cmd/main.go &
APP_PID=$!
sleep 2

# 3. Hey тестирование (~5 минут)
hey -z 60s -c 50 http://localhost:8080/api/v1/top
hey -z 60s -c 100 http://localhost:8080/api/v1/top
hey -z 60s -c 100 -q 100 http://localhost:8080/api/v1/top

# 4. Vegeta тестирование (~5 минут)
echo "GET http://localhost:8080/api/v1/top" | \
  vegeta attack -duration=30s -rate=100 | vegeta report

echo "GET http://localhost:8080/api/v1/top" | \
  vegeta attack -duration=30s -rate=500 | vegeta report

# 5. WRK тестирование (~10 минут)
wrk -t 4 -c 50 -d 60s http://localhost:8080/api/v1/top
wrk -t 4 -c 100 -d 60s http://localhost:8080/api/v1/top

# Очистка
kill $APP_PID
```

## 📊 Сравнение инструментов

| Инструмент | Установка | Простота | Детали | Лучше всего для |
|-----------|-----------|---------|--------|-----------------|
| **Go test** | Встроенный | Очень просто | Точные микро-тесты | Профилирование |
| **Hey** | `go install` | Просто | Базовые метрики | Быстрое тестирование |
| **Vegeta** | `go install` | Средне | JSON экспорт | Анализ распределения |
| **WRK** | Apt/Brew | Сложно | Детальные метрики | Продакшн нагрузка |

## ⚡ Quick Commands

```bash
# Быстрый бенчмарк (30 секунд)
go test -bench=BenchmarkTopEndpoint -benchtime=5s -benchmem ./internal/api

# Стресс-тест (давление 100 concurrent 30 секунд)
hey -z 30s -c 100 http://localhost:8080/api/v1/top

# Vegeta attack (500 запросов в секунду 30 секунд)
echo "GET http://localhost:8080/api/v1/top" | vegeta attack -rate=500 -duration=30s | vegeta report

# WRK (4 потока, 100 соединений, 60 секунд)
wrk -t 4 -c 100 -d 60s http://localhost:8080/api/v1/top

# Долгосрочный стресс-тест (Go)
go test -run=TestStressLoad -v -timeout=120s ./internal/api
```

## 🔍 Определение проблем

### Низкий RPS (< 5000 req/sec)

```bash
# Проверить CPU profiling
go test -bench=. -cpuprofile=cpu.prof ./internal/api
go tool pprof cpu.prof

# Или memory profiling
go test -bench=. -memprofile=mem.prof ./internal/api
```

### Высокая latency (> 50ms)

```bash
# Проверить goroutine leaks
go test -race -run=. ./internal/api

# Проверить memory leaks
go test -bench=. -benchmem ./internal/api
# Если allocations растут - есть утечка
```

### Растущая error rate

```bash
# Включить debug логирование
slog.SetLogLoggerLevel(slog.LevelDebug)

# Запустить тест с verbose
go test -run=TestStressLoad -v ./internal/api
```

---

**📚 Подробное руководство:** [LOAD_TESTING_GUIDE.md](./LOAD_TESTING_GUIDE.md)
