#!/bin/bash

# Специализированный сценарий нагрузочного тестирования с wrk
# Требует установки wrk: https://github.com/wg/wrk

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${PROJECT_DIR}/benchmark_results"
WRK_RESULTS="${RESULTS_DIR}/wrk"

mkdir -p "$WRK_RESULTS"

echo "════════════════════════════════════════════════════════════"
echo "     WRK Load Testing Suite"
echo "════════════════════════════════════════════════════════════"
echo ""

# Check if wrk is installed
if ! command -v wrk &> /dev/null; then
    echo "❌ wrk is not installed"
    echo ""
    echo "Installation instructions:"
    echo "  macOS:     brew install wrk"
    echo "  Ubuntu:    sudo apt-get install wrk"
    echo "  Arch:      sudo pacman -S wrk"
    echo "  From source: https://github.com/wg/wrk"
    echo ""
    exit 1
fi

# Start application
echo "Starting application..."
cd "$PROJECT_DIR"
go run ./cmd/main.go > "$RESULTS_DIR/app.log" 2>&1 &
APP_PID=$!
sleep 3

trap "kill $APP_PID 2>/dev/null || true" EXIT

SERVER_URL="http://localhost:8080"

# Check if server is running
if ! curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
    echo "❌ Application failed to start"
    exit 1
fi

echo "✓ Application running at $SERVER_URL"
echo ""

# Get number of CPU cores
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
echo "CPU Cores: $CORES"
echo ""

# ────────────────────────────────────────────────────────────────
# Test 1: Single-threaded baseline
# ────────────────────────────────────────────────────────────────

echo "Test 1: Single-threaded Baseline (30 seconds)"
echo "─────────────────────────────────────────────"

wrk -t 1 \
    -c 1 \
    -d 30s \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/01_baseline_1t1c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 2: Light concurrent load
# ────────────────────────────────────────────────────────────────

echo "Test 2: Light Load (4 threads, 10 connections, 30s)"
echo "──────────────────────────────────────────────────"

wrk -t 4 \
    -c 10 \
    -d 30s \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/02_light_4t10c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 3: Medium load
# ────────────────────────────────────────────────────────────────

echo "Test 3: Medium Load (4 threads, 50 connections, 60s)"
echo "───────────────────────────────────────────────────"

wrk -t 4 \
    -c 50 \
    -d 60s \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/03_medium_4t50c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 4: High load
# ────────────────────────────────────────────────────────────────

echo "Test 4: High Load (4 threads, 100 connections, 60s)"
echo "───────────────────────────────────────────────────"

wrk -t 4 \
    -c 100 \
    -d 60s \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/04_high_4t100c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 5: Very high load
# ────────────────────────────────────────────────────────────────

echo "Test 5: Very High Load (8 threads, 200 connections, 60s)"
echo "────────────────────────────────────────────────────────"

wrk -t 8 \
    -c 200 \
    -d 60s \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/05_veryhigh_8t200c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 6: With custom Lua script (latency distribution)
# ────────────────────────────────────────────────────────────────

echo "Test 6: With Latency Tracking Script"
echo "────────────────────────────────────"

# Create Lua script for latency tracking
cat > "$WRK_RESULTS/latency_tracking.lua" << 'LUA'
request = function()
   return wrk.format(nil, "/api/v1/top")
end

response = function(status, headers, body)
   if status ~= 200 then
      io.stderr:write("HTTP " .. status .. "\n")
   end
end
LUA

wrk -t 4 \
    -c 50 \
    -d 30s \
    -s "$WRK_RESULTS/latency_tracking.lua" \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/06_latency_tracking_4t50c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 7: With query parameters (?limit=10)
# ────────────────────────────────────────────────────────────────

echo "Test 7: With Query Parameters (?limit=10)"
echo "─────────────────────────────────────────"

cat > "$WRK_RESULTS/with_limit.lua" << 'LUA'
request = function()
   return wrk.format(nil, "/api/v1/top?limit=10")
end
LUA

wrk -t 4 \
    -c 50 \
    -d 30s \
    -s "$WRK_RESULTS/with_limit.lua" \
    "$SERVER_URL/api/v1/top?limit=10" \
    | tee "$WRK_RESULTS/07_with_limit_4t50c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 8: Comparison - hot path vs health endpoint
# ────────────────────────────────────────────────────────────────

echo "Test 8: Comparison - Hot Path vs Health Endpoint"
echo "────────────────────────────────────────────────"

echo "Testing /health endpoint..."
wrk -t 4 \
    -c 50 \
    -d 30s \
    "$SERVER_URL/health" \
    | tee "$WRK_RESULTS/08_health_4t50c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 9: Sustained load with duration analysis
# ────────────────────────────────────────────────────────────────

echo "Test 9: Sustained Load (300 seconds, 4 threads, 100 conn)"
echo "────────────────────────────────────────────────────────"

wrk -t 4 \
    -c 100 \
    -d 300s \
    "$SERVER_URL/api/v1/top" \
    | tee "$WRK_RESULTS/09_sustained_4t100c_5min.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Parse and create summary
# ────────────────────────────────────────────────────────────────

echo "Creating summary report..."
echo ""

SUMMARY_FILE="$WRK_RESULTS/SUMMARY.md"

cat > "$SUMMARY_FILE" << 'EOF'
# WRK Load Testing Results

## Overview

WRK is a modern HTTP benchmarking tool written in C with an embedded Lua scripting capability.

### Test Matrix

| Test | Threads | Connections | Duration | Endpoint |
|------|---------|-------------|----------|----------|
| 1 | 1 | 1 | 30s | /api/v1/top |
| 2 | 4 | 10 | 30s | /api/v1/top |
| 3 | 4 | 50 | 60s | /api/v1/top |
| 4 | 4 | 100 | 60s | /api/v1/top |
| 5 | 8 | 200 | 60s | /api/v1/top |
| 6 | 4 | 50 | 30s | /api/v1/top (with latency tracking) |
| 7 | 4 | 50 | 30s | /api/v1/top?limit=10 |
| 8 | 4 | 50 | 30s | /health |
| 9 | 4 | 100 | 300s | /api/v1/top (sustained) |

## Interpretation Guide

### Typical WRK Output Example

```
Running 30s test @ http://localhost:8080/api/v1/top
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max    +/- Stdev
    Latency     2.43ms   1.82ms   45.67ms   88.94%
    Req/Sec     5.10k    0.82k    7.50k    85.04%
  Latencies     25634 requests in 30.02s, 13.45MB read
    50%      2ms
    75%      3ms
    90%      5ms
    99%     10ms
    99.9%    20ms
  Requests:      51200
  Transfer:      26.89MB
  Requests/sec:  1706.59
  Transfer/sec:  895.41KB
```

### Metrics Explanation

- **Latency (Avg)**: Average response time
- **Latency (Stdev)**: Standard deviation (variability)
- **Latency (Max)**: Maximum response time observed
- **Req/Sec (Avg)**: Average requests per second per thread
- **50%/75%/90%/99% Latencies**: Percentile latencies
- **Requests/sec**: Total throughput
- **Transfer/sec**: Throughput in bytes

## Key Performance Indicators

### Expected Metrics for Hot Path

- **Requests/sec**: > 10,000
- **Latency (p50)**: < 2ms
- **Latency (p99)**: < 10ms
- **Transfer/sec**: Depends on payload size
- **Error Rate**: 0%

EOF

echo "View results:"
for f in "$WRK_RESULTS"/*.txt; do
    if [[ -f "$f" ]]; then
        echo "  $(basename $f)"
    fi
done

echo ""

# Extract key metrics from each test
echo "Quick Summary:"
echo "────────────"
echo ""

for txt_file in "$WRK_RESULTS"/*.txt; do
    if [[ -f "$txt_file" ]]; then
        test_name=$(basename "$txt_file" .txt)
        rps=$(grep "Requests/sec" "$txt_file" | awk '{print $2}')
        latency=$(grep "Latency" "$txt_file" | head -1 | awk '{print $2}')
        
        if [[ ! -z "$rps" ]] && [[ ! -z "$latency" ]]; then
            printf "%-40s | RPS: %8s | Latency: %s\n" "$test_name" "$rps" "$latency"
        fi
    fi
done

echo ""

# ────────────────────────────────────────────────────────────────
# Final Summary
# ────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════"
echo "        WRK Testing Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📁 Results saved to: $WRK_RESULTS/"
echo ""
echo "View detailed results:"
echo "  cat $WRK_RESULTS/04_high_4t100c.txt"
echo "  cat $WRK_RESULTS/SUMMARY.md"
echo ""
