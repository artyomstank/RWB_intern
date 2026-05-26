package anomaly_test

import (
	"testing"

	"github.com/artyomstank/RWB_intern/internal/anomaly"
)

const testHash = uint64(0xdeadbeef)

func TestIsAnomaly_BelowMinCount(t *testing.T) {
	d := anomaly.New()
	if d.IsAnomaly(testHash, 5, 0) {
		t.Error("expected no anomaly for count below minimum threshold")
	}
}

func TestIsAnomaly_LowDiversity(t *testing.T) {
	d := anomaly.New()
	if !d.IsAnomaly(testHash, 100, 2) {
		t.Error("expected anomaly for low diversity ratio (bot traffic)")
	}
}

func TestIsAnomaly_ZeroDiversity(t *testing.T) {
	d := anomaly.New()
	if !d.IsAnomaly(testHash, 100, 0) {
		t.Error("expected anomaly for zero unique users")
	}
}

func TestIsAnomaly_HighDiversity_FirstCall(t *testing.T) {
	d := anomaly.New()
	if d.IsAnomaly(testHash, 100, 80) {
		t.Error("expected no anomaly on first call with good diversity")
	}
}

func TestIsAnomaly_VelocitySpike(t *testing.T) {
	d := anomaly.New()

	for range 10 {
		d.IsAnomaly(testHash, 100, 90)
	}

	if !d.IsAnomaly(testHash, 1000, 900) {
		t.Error("expected anomaly for 10x velocity spike over established EMA")
	}
}

func TestIsAnomaly_NoSpikeNormalGrowth(t *testing.T) {
	d := anomaly.New()

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

	d.IsAnomaly(1, 100, 90)
	d.IsAnomaly(2, 100, 90)

	if !d.IsAnomaly(1, 1000, 900) {
		t.Error("expected spike for hash 1")
	}
	if d.IsAnomaly(2, 110, 99) {
		t.Error("expected no anomaly for hash 2 (normal growth)")
	}
}

func TestIsAnomaly_SetMinDiversity(t *testing.T) {
	d := anomaly.New()
	d.SetMinDiversity(0.01)

	if d.IsAnomaly(testHash, 100, 3) {
		t.Error("expected no anomaly after lowering diversity threshold to 0.01")
	}
}

func TestIsAnomaly_Reset(t *testing.T) {
	d := anomaly.New()

	for range 5 {
		d.IsAnomaly(testHash, 100, 90)
	}

	d.Reset()

	if d.IsAnomaly(testHash, 1000, 900) {
		t.Error("expected no anomaly on first call after Reset (EMA reinitialized)")
	}
}
