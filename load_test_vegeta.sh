#!/bin/bash

# Специализированный сценарий нагрузочного тестирования с vegeta
# Требует: go install github.com/tsenart/vegeta@latest

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${PROJECT_DIR}/benchmark_results"
VEGETA_RESULTS="${RESULTS_DIR}/vegeta"

mkdir -p "$VEGETA_RESULTS"

echo "════════════════════════════════════════════════════════════"
echo "     Vegeta Load Testing Suite"
echo "════════════════════════════════════════════════════════════"
echo ""

# Check if vegeta is installed
if ! command -v vegeta &> /dev/null; then
    echo "❌ vegeta is not installed"
    echo "Install it with: go install github.com/tsenart/vegeta@latest"
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
# Test 1: Baseline (50 req/sec, 30 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 1: Baseline (50 req/sec, 30 seconds)"
echo "──────────────────────────────────────────"

DURATION=30s
RATE=50

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta report > "$VEGETA_RESULTS/01_baseline_50rps.txt"

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta dump --dumper=json > "$VEGETA_RESULTS/01_baseline_50rps.json"

cat "$VEGETA_RESULTS/01_baseline_50rps.txt"
echo ""

# ────────────────────────────────────────────────────────────────
# Test 2: Medium load (200 req/sec, 30 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 2: Medium Load (200 req/sec, 30 seconds)"
echo "──────────────────────────────────────────────"

RATE=200

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta report > "$VEGETA_RESULTS/02_medium_200rps.txt"

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta dump --dumper=json > "$VEGETA_RESULTS/02_medium_200rps.json"

cat "$VEGETA_RESULTS/02_medium_200rps.txt"
echo ""

# ────────────────────────────────────────────────────────────────
# Test 3: High load (500 req/sec, 30 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 3: High Load (500 req/sec, 30 seconds)"
echo "────────────────────────────────────────────"

RATE=500

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta report > "$VEGETA_RESULTS/03_high_500rps.txt"

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta dump --dumper=json > "$VEGETA_RESULTS/03_high_500rps.json"

cat "$VEGETA_RESULTS/03_high_500rps.txt"
echo ""

# ────────────────────────────────────────────────────────────────
# Test 4: Peak load (1000 req/sec, 30 seconds)
# ────────────────────────────────────────────────────────────────

echo "Test 4: Peak Load (1000 req/sec, 30 seconds)"
echo "─────────────────────────────────────────────"

RATE=1000

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta report > "$VEGETA_RESULTS/04_peak_1000rps.txt"

echo "GET $SERVER_URL/api/v1/top" | \
vegeta attack -duration=$DURATION -rate=$RATE | \
vegeta dump --dumper=json > "$VEGETA_RESULTS/04_peak_1000rps.json"

cat "$VEGETA_RESULTS/04_peak_1000rps.txt"
echo ""

# ────────────────────────────────────────────────────────────────
# Test 5: Burst (variable rate)
# ────────────────────────────────────────────────────────────────

echo "Test 5: Burst Test (varying rate)"
echo "──────────────────────────────────"

# Create attackfile with varying rates
cat > "$VEGETA_RESULTS/burst.txt" << 'ATTACK'
GET http://localhost:8080/api/v1/top
X-Burst: 1

GET http://localhost:8080/api/v1/top
X-Burst: 2

GET http://localhost:8080/api/v1/top
X-Burst: 3
ATTACK

vegeta attack -rate=1000/1s -duration=30s -targets="$VEGETA_RESULTS/burst.txt" | \
vegeta report > "$VEGETA_RESULTS/05_burst.txt"

cat "$VEGETA_RESULTS/05_burst.txt"
echo ""

# ────────────────────────────────────────────────────────────────
# Test 6: Mixed endpoints
# ────────────────────────────────────────────────────────────────

echo "Test 6: Mixed Endpoints (hot path + health)"
echo "────────────────────────────────────────────"

cat > "$VEGETA_RESULTS/mixed.txt" << 'ATTACK'
GET http://localhost:8080/api/v1/top

GET http://localhost:8080/api/v1/top?limit=10

GET http://localhost:8080/health
ATTACK

vegeta attack -rate=500/1s -duration=30s -targets="$VEGETA_RESULTS/mixed.txt" | \
vegeta report > "$VEGETA_RESULTS/06_mixed.txt"

cat "$VEGETA_RESULTS/06_mixed.txt"
echo ""

# ────────────────────────────────────────────────────────────────
# Generate plots (if gnuplot available)
# ────────────────────────────────────────────────────────────────

echo "Test 7: Plot generation"
echo "───────────────────────"

if command -v gnuplot &> /dev/null; then
    echo "Generating plots..."
    
    for f in "$VEGETA_RESULTS"/*.json; do
        if [ -f "$f" ]; then
            name=$(basename "$f" .json)
            vegeta report -type=text "$f" > "$VEGETA_RESULTS/${name}_report.txt"
            echo "✓ Generated report for $name"
        fi
    done
else
    echo "⚠️  gnuplot not available (optional for plot generation)"
fi

echo ""

# ────────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════"
echo "        Vegeta Testing Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📁 Results saved to: $VEGETA_RESULTS/"
echo ""
echo "Files generated:"
ls -lh "$VEGETA_RESULTS"/ | tail -n +2 | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "View detailed results:"
echo "  cat $VEGETA_RESULTS/03_high_500rps.txt"
echo ""
