package anomaly_test

import (
	"testing"

	"github.com/artyomstank/RWB_intern/internal/anomaly"
)

const testHash = uint64(0xdeadbeef)

func TestIsAnomaly_BelowMinCount(t *testing.T) {
	d := anomaly.New()
	// count=5 < minCountForCheck=10 → никаких проверок
	if d.IsAnomaly(testHash, 5, 0) {
		t.Error("expected no anomaly for count below minimum threshold")
	}
}

func TestIsAnomaly_LowDiversity(t *testing.T) {
	d := anomaly.New()
	// count=100, uniqueUsers=2 → diversity=0.02 < 0.05 → аномалия
	if !d.IsAnomaly(testHash, 100, 2) {
		t.Error("expected anomaly for low diversity ratio (bot traffic)")
	}
}

func TestIsAnomaly_ZeroDiversity(t *testing.T) {
	d := anomaly.New()
	// count=100, uniqueUsers=0 — edge case HLL вернул 0
	if !d.IsAnomaly(testHash, 100, 0) {
		t.Error("expected anomaly for zero unique users")
	}
}

func TestIsAnomaly_HighDiversity_FirstCall(t *testing.T) {
	d := anomaly.New()
	// Первый вызов — EMA инициализируется, spike не срабатывает.
	if d.IsAnomaly(testHash, 100, 80) {
		t.Error("expected no anomaly on first call with good diversity")
	}
}

func TestIsAnomaly_VelocitySpike(t *testing.T) {
	d := anomaly.New()

	// Устанавливаем стабильную EMA ≈ 100 несколькими вызовами.
	for range 10 {
		d.IsAnomaly(testHash, 100, 90)
	}

	// Внезапный spike в 10× — должен сработать (spikeRatio default = 8).
	if !d.IsAnomaly(testHash, 1000, 900) {
		t.Error("expected anomaly for 10x velocity spike over established EMA")
	}
}

func TestIsAnomaly_NoSpikeNormalGrowth(t *testing.T) {
	d := anomaly.New()

	// Органический рост: 100 → 150 → 200 → 250.
	// EMA следует за трендом, spike не срабатывает.
	counts := []int32{100, 150, 200, 250}
	for _, c := range counts {
		uniq := uint64(float64(c) * 0.9)
		if d.IsAnomaly(testHash, int64(c), uniq) {
			t.Errorf("unexpected anomaly for organic growth at count=%d", c)
		}
	}
}

func TestIsAnomaly_MultipleHashes_Independent(t *testing.T) {
	d := anomaly.New()

	// Разные запросы имеют независимые EMA.
	d.IsAnomaly(1, 100, 90)
	d.IsAnomaly(2, 100, 90)

	// Spike для хэша 1 не должен влиять на хэш 2.
	if !d.IsAnomaly(1, 1000, 900) {
		t.Error("expected spike for hash 1")
	}
	if d.IsAnomaly(2, 110, 99) {
		t.Error("expected no anomaly for hash 2 (normal growth)")
	}
}

func TestIsAnomaly_SetMinDiversity(t *testing.T) {
	d := anomaly.New()
	// Снижаем порог: diversity=0.03 при threshold=0.01 — не аномалия.
	d.SetMinDiversity(0.01)

	if d.IsAnomaly(testHash, 100, 3) {
		t.Error("expected no anomaly after lowering diversity threshold to 0.01")
	}
}

func TestIsAnomaly_Reset(t *testing.T) {
	d := anomaly.New()

	// Устанавливаем EMA.
	for range 5 {
		d.IsAnomaly(testHash, 100, 90)
	}

	d.Reset()

	// После Reset первый вызов — инициализация, не spike.
	if d.IsAnomaly(testHash, 1000, 900) {
		// После Reset EMA = 1000 (инициализация), spike не срабатывает.
		t.Error("expected no anomaly on first call after Reset (EMA reinitialized)")
	}
}
