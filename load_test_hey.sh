#!/bin/bash

# Специализированный сценарий нагрузочного тестирования с hey
# Требует: go install github.com/rakyll/hey@latest

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${PROJECT_DIR}/benchmark_results"
HEY_RESULTS="${RESULTS_DIR}/hey"

mkdir -p "$HEY_RESULTS"

echo "════════════════════════════════════════════════════════════"
echo "     Hey Load Testing Suite"
echo "════════════════════════════════════════════════════════════"
echo ""

# Check if hey is installed
if ! command -v hey &> /dev/null; then
    echo "❌ hey is not installed"
    echo "Install it with: go install github.com/rakyll/hey@latest"
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

# ────────────────────────────────────────────────────────────────
# Test 1: Light load (10 concurrent, 10 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 1: Light Load (10 concurrent, 10 seconds)"
echo "──────────────────────────────────────────────"

hey -n 1000 \
    -c 10 \
    -z 10s \
    -o "$HEY_RESULTS/01_light_10c.csv" \
    "$SERVER_URL/api/v1/top" \
    | tee "$HEY_RESULTS/01_light_10c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 2: Medium load (50 concurrent, 30 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 2: Medium Load (50 concurrent, 30 seconds)"
echo "───────────────────────────────────────────────"

hey -n 5000 \
    -c 50 \
    -z 30s \
    -o "$HEY_RESULTS/02_medium_50c.csv" \
    "$SERVER_URL/api/v1/top" \
    | tee "$HEY_RESULTS/02_medium_50c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 3: High load (100 concurrent, 60 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 3: High Load (100 concurrent, 60 seconds)"
echo "──────────────────────────────────────────────"

hey -n 10000 \
    -c 100 \
    -z 60s \
    -o "$HEY_RESULTS/03_high_100c.csv" \
    "$SERVER_URL/api/v1/top" \
    | tee "$HEY_RESULTS/03_high_100c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 4: Very high load (200 concurrent, 60 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 4: Very High Load (200 concurrent, 60 seconds)"
echo "───────────────────────────────────────────────────"

hey -n 20000 \
    -c 200 \
    -z 60s \
    -o "$HEY_RESULTS/04_veryhigh_200c.csv" \
    "$SERVER_URL/api/v1/top" \
    | tee "$HEY_RESULTS/04_veryhigh_200c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 5: With limit parameter
# ────────────────────────────────────────────────────────────────

echo "Test 5: With limit parameter (?limit=10)"
echo "───────────────────────────────────────────"

hey -n 5000 \
    -c 50 \
    -z 30s \
    -o "$HEY_RESULTS/05_limit10_50c.csv" \
    "$SERVER_URL/api/v1/top?limit=10" \
    | tee "$HEY_RESULTS/05_limit10_50c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 6: Health endpoint (baseline)
# ────────────────────────────────────────────────────────────────

echo "Test 6: Health Endpoint (baseline, 100 concurrent)"
echo "──────────────────────────────────────────────────"

hey -n 5000 \
    -c 100 \
    -z 30s \
    -o "$HEY_RESULTS/06_health_100c.csv" \
    "$SERVER_URL/health" \
    | tee "$HEY_RESULTS/06_health_100c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 7: Request rate limiting
# ────────────────────────────────────────────────────────────────

echo "Test 7: Rate Limited (500 req/sec, 100 concurrent)"
echo "───────────────────────────────────────────────────"

hey -n 5000 \
    -c 100 \
    -q 500 \
    -z 30s \
    -o "$HEY_RESULTS/07_rate500_100c.csv" \
    "$SERVER_URL/api/v1/top" \
    | tee "$HEY_RESULTS/07_rate500_100c.txt"

echo ""

# ────────────────────────────────────────────────────────────────
# Test 8: Spiking pattern (ramping up load)
# ────────────────────────────────────────────────────────────────

echo "Test 8: Progressive Load Increase"
echo "─────────────────────────────────"

for concurrency in 10 25 50 75 100; do
    echo "  → Testing with $concurrency concurrent connections..."
    hey -n 1000 \
        -c $concurrency \
        -z 10s \
        -o "$HEY_RESULTS/08_progressive_${concurrency}c.csv" \
        "$SERVER_URL/api/v1/top" \
        >> "$HEY_RESULTS/08_progressive.txt" 2>&1
done

echo ""

# ────────────────────────────────────────────────────────────────
# Generate summary statistics
# ────────────────────────────────────────────────────────────────

echo "Test 9: Summary Statistics"
echo "──────────────────────────"
echo ""

SUMMARY_FILE="$HEY_RESULTS/SUMMARY.txt"

cat > "$SUMMARY_FILE" << 'EOF'
# Hey Load Testing Summary

## Test Results Overview

EOF

for txt_file in "$HEY_RESULTS"/*.txt; do
    if [[ "$txt_file" != *"SUMMARY"* ]] && [[ -f "$txt_file" ]]; then
        name=$(basename "$txt_file" .txt)
        echo "" >> "$SUMMARY_FILE"
        echo "## Test: $name" >> "$SUMMARY_FILE"
        echo "---" >> "$SUMMARY_FILE"
        grep -E "Requests|Latencies|Throughput|Status|errors" "$txt_file" >> "$SUMMARY_FILE" || true
    fi
done

cat "$SUMMARY_FILE"
echo ""

# ────────────────────────────────────────────────────────────────
# Final Summary
# ────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════"
echo "        Hey Testing Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📁 Results saved to: $HEY_RESULTS/"
echo ""
echo "Files generated:"
ls -lh "$HEY_RESULTS"/ | tail -n +2 | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "View detailed results:"
echo "  cat $HEY_RESULTS/03_high_100c.txt"
echo ""
echo "Note: CSV files can be imported into Excel/Sheets for analysis"
echo ""
