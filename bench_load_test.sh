#!/bin/bash

# Комплексный скрипт для нагрузочного тестирования
# Использует встроенные Go benchmarks и внешние инструменты

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${PROJECT_DIR}/benchmark_results"

mkdir -p "$RESULTS_DIR"

echo "════════════════════════════════════════════════════════════"
echo "     Load Testing & Benchmarking Suite"
echo "════════════════════════════════════════════════════════════"
echo ""

# ────────────────────────────────────────────────────────────────
# 1. GO BUILT-IN BENCHMARKS
# ────────────────────────────────────────────────────────────────

echo "📊 Phase 1: Go Built-in Benchmarks"
echo "───────────────────────────────────"
echo ""

cd "$PROJECT_DIR"

echo "Running API benchmarks (hot path)..."
go test -bench=BenchmarkTopEndpoint \
    -benchmem \
    -benchtime=10s \
    -run=^$ \
    ./internal/api \
    | tee "$RESULTS_DIR/01_benchmark_top_endpoint.txt"

echo ""
echo "Running concurrent benchmarks..."
go test -bench=BenchmarkConcurrentRequests \
    -benchmem \
    -benchtime=5s \
    -run=^$ \
    ./internal/api \
    | tee "$RESULTS_DIR/02_benchmark_concurrent.txt"

echo ""
echo "Running response payload benchmarks..."
go test -bench=BenchmarkResponsePayload \
    -benchmem \
    -benchtime=5s \
    -run=^$ \
    ./internal/api \
    | tee "$RESULTS_DIR/03_benchmark_payload.txt"

echo ""
echo "Running stoplist operations benchmarks..."
go test -bench=BenchmarkStoplistOperations \
    -benchmem \
    -benchtime=5s \
    -run=^$ \
    ./internal/api \
    | tee "$RESULTS_DIR/04_benchmark_stoplist.txt"

# ────────────────────────────────────────────────────────────────
# 2. STRESS & LOAD TESTS
# ────────────────────────────────────────────────────────────────

echo ""
echo "📊 Phase 2: Stress & Load Tests"
echo "────────────────────────────────"
echo ""

echo "Running stress test (30 seconds, 100 concurrent)..."
go test -run=TestStressLoad \
    -timeout=60s \
    -v \
    ./internal/api \
    | tee "$RESULTS_DIR/05_stress_test.txt"

echo ""
echo "Running load metrics test (5000 requests)..."
go test -run=TestLoadMetrics \
    -timeout=120s \
    -v \
    ./internal/api \
    | tee "$RESULTS_DIR/06_load_metrics.txt"

# ────────────────────────────────────────────────────────────────
# 3. EXTERNAL TOOLS (if installed)
# ────────────────────────────────────────────────────────────────

echo ""
echo "📊 Phase 3: External Load Testing Tools"
echo "────────────────────────────────────────"
echo ""

# Start application in background for external tool tests
echo "Starting application for external tool tests..."
go run ./cmd/main.go > "$RESULTS_DIR/app.log" 2>&1 &
APP_PID=$!
sleep 3

SERVER_URL="http://localhost:8080"

# Check if server is running
if ! curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
    echo "⚠️  Application not running, skipping external tool tests"
    kill $APP_PID 2>/dev/null || true
else
    echo "✓ Application is running at $SERVER_URL"
    echo ""

    # Test with 'hey' if available
    if command -v hey &> /dev/null; then
        echo "Running hey benchmark (1000 requests, 100 concurrent)..."
        hey -z 30s \
            -c 100 \
            -q 100 \
            "$SERVER_URL/api/v1/top" \
            | tee "$RESULTS_DIR/07_hey_results.txt"
        echo ""
    else
        echo "⚠️  'hey' not installed. To install: go install github.com/rakyll/hey@latest"
    fi

    # Test with 'vegeta' if available
    if command -v vegeta &> /dev/null; then
        echo "Running vegeta attack (30 seconds, 100 requests/sec)..."
        
        echo "GET $SERVER_URL/api/v1/top" | \
        vegeta attack -duration=30s -rate=100 | \
        vegeta report > "$RESULTS_DIR/08_vegeta_report.txt" 2>&1
        
        echo "Vegeta results saved"
        cat "$RESULTS_DIR/08_vegeta_report.txt"
        echo ""
    else
        echo "⚠️  'vegeta' not installed. To install: go install github.com/tsenart/vegeta@latest"
    fi

    # Test with 'wrk' if available
    if command -v wrk &> /dev/null; then
        echo "Running wrk benchmark (30 seconds, 100 concurrent)..."
        wrk -t 4 \
            -c 100 \
            -d 30s \
            "$SERVER_URL/api/v1/top" \
            | tee "$RESULTS_DIR/09_wrk_results.txt"
        echo ""
    else
        echo "⚠️  'wrk' not installed. To install: see https://github.com/wg/wrk"
    fi

    # Cleanup
    kill $APP_PID 2>/dev/null || true
    sleep 1
fi

# ────────────────────────────────────────────────────────────────
# 4. GENERATE SUMMARY REPORT
# ────────────────────────────────────────────────────────────────

echo ""
echo "📊 Phase 4: Generating Summary Report"
echo "─────────────────────────────────────"
echo ""

SUMMARY_FILE="$RESULTS_DIR/SUMMARY.md"

cat > "$SUMMARY_FILE" << 'EOF'
# Load Testing & Benchmarking Results

## Test Overview

This report contains comprehensive benchmarking and load testing results for the Trending Queries Service.

### Tests Executed

1. **Go Built-in Benchmarks** - Precise micro-benchmarks using Go's testing framework
2. **Stress Tests** - Long-running tests with high concurrency (100 concurrent, 30 seconds)
3. **Load Metrics** - Latency percentiles and throughput analysis
4. **External Tools** - Optional tests using `hey`, `vegeta`, and `wrk` for real HTTP testing

---

## Results Files

### Go Benchmarks
- `01_benchmark_top_endpoint.txt` - GET /api/v1/top endpoint
- `02_benchmark_concurrent.txt` - Concurrent request handling (1, 10, 50, 100 goroutines)
- `03_benchmark_payload.txt` - Response payload sizes (10-1000 items)
- `04_benchmark_stoplist.txt` - Stoplist operations

### Stress & Load Tests
- `05_stress_test.txt` - 30-second stress test (100 concurrent)
- `06_load_metrics.txt` - Latency percentiles (5000 requests, 50 concurrent)

### External Tools (if available)
- `07_hey_results.txt` - Hey benchmark results
- `08_vegeta_report.txt` - Vegeta attack report
- `09_wrk_results.txt` - WRK benchmark results

---

## Key Metrics

### Expected Performance

Based on optimized hot path (`atomic.Load` + pre-serialized bytes):

- **Requests/sec**: 50,000+ (single core)
- **Latency (p50)**: < 1ms
- **Latency (p95)**: < 5ms
- **Latency (p99)**: < 10ms
- **Memory alloc per request**: 0 bytes (hot path)

### Architecture Strengths

✓ **Concurrent-safe**: Uses `atomic.Load` for zero-lock reads  
✓ **Zero-allocation hot path**: Pre-serialized JSON stored in cache  
✓ **Efficient worker goroutines**: Controlled concurrency in Kafka consumer  
✓ **Optional request limiting**: ?limit=N parameter for mobile clients  

---

## Running the Tests

### Quick Run (built-in benchmarks only)
```bash
./bench_load_test.sh
```

### Install External Tools

```bash
# hey - simple HTTP benchmark
go install github.com/rakyll/hey@latest

# vegeta - load testing tool
go install github.com/tsenart/vegeta@latest

# wrk - HTTP benchmarking tool (requires build)
# See: https://github.com/wg/wrk
```

### Manual Testing

```bash
# Start the application
go run ./cmd/main.go

# In another terminal, test with hey
hey -z 60s -c 100 -q 50 http://localhost:8080/api/v1/top

# Or with vegeta
echo "GET http://localhost:8080/api/v1/top" | vegeta attack -duration=60s -rate=100 | vegeta report

# Or with wrk
wrk -t 4 -c 100 -d 60s http://localhost:8080/api/v1/top
```

---

## Interpretation Guide

### Go Benchmark Output Example
```
BenchmarkTopEndpoint-8    48372    24803 ns/op    1024 B/op    8 allocs/op
```
- `48372` = operations completed
- `24803 ns/op` = nanoseconds per operation
- `1024 B/op` = bytes allocated per operation
- `8 allocs/op` = number of allocations per operation

### Load Test Output Example
- **Successful requests**: Total completed without error
- **Requests/sec**: Throughput
- **Min/Avg/Max Latency**: Response time distribution
- **P95/P99 Latency**: 95th and 99th percentile latencies

---

## Recommendations

1. **Monitor P99 Latency**: Aim for < 50ms under sustained load
2. **Memory Usage**: Watch for steady increase (possible memory leak)
3. **Error Rate**: Should remain < 0.1% under normal conditions
4. **Connection Pool**: Tune transport settings based on results

---

Generated: $(date)
EOF

echo "✓ Summary report created: $SUMMARY_FILE"
echo ""

# ────────────────────────────────────────────────────────────────
# 5. DISPLAY SUMMARY
# ────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════"
echo "        Testing Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📁 Results saved to: $RESULTS_DIR/"
echo ""
echo "Key Results:"
echo "─────────────"

if [ -f "$RESULTS_DIR/01_benchmark_top_endpoint.txt" ]; then
    echo ""
    echo "🔥 Hot Path (/api/v1/top):"
    grep -E "BenchmarkTopEndpoint|ns/op|allocs" "$RESULTS_DIR/01_benchmark_top_endpoint.txt" | head -5
fi

if [ -f "$RESULTS_DIR/05_stress_test.txt" ]; then
    echo ""
    echo "💪 Stress Test (30s, 100 concurrent):"
    grep -E "Successful|Failed|Requests/sec" "$RESULTS_DIR/05_stress_test.txt" | head -5
fi

if [ -f "$RESULTS_DIR/06_load_metrics.txt" ]; then
    echo ""
    echo "📊 Load Metrics:"
    grep -E "Total Requests|Min|Max|Avg|P95|P99" "$RESULTS_DIR/06_load_metrics.txt" | head -10
fi

echo ""
echo "To view full results, open:"
echo "  - $RESULTS_DIR/SUMMARY.md"
echo "  - Individual result files in $RESULTS_DIR/"
echo ""
