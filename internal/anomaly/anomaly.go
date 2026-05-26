package anomaly

import (
	"sync"
)

const (
	minCountForCheck = 10

	defaultMinDiversity = 0.05
	defaultEMAAlpha     = 0.15
	defaultSpikeRatio   = 8.0
)

type queryState struct {
	ema float64
}

type Detector struct {
	mu           sync.Mutex
	states       map[uint64]*queryState
	minDiversity float64
	emaAlpha     float64
	spikeRatio   float64
}

func New() *Detector {
	return &Detector{
		states:       make(map[uint64]*queryState),
		minDiversity: defaultMinDiversity,
		emaAlpha:     defaultEMAAlpha,
		spikeRatio:   defaultSpikeRatio,
	}
}

func (d *Detector) IsAnomaly(h uint64, count int64, uniqueUsers uint64) bool {
	if count < minCountForCheck {
		return false
	}

	cnt := float64(count)
	uniq := float64(uniqueUsers)

	if uniq/cnt < d.minDiversity {
		return true
	}

	d.mu.Lock()
	st, ok := d.states[h]
	if !ok {
		d.states[h] = &queryState{ema: cnt}
		d.mu.Unlock()
		return false
	}

	prevEMA := st.ema
	isSpike := prevEMA > 0 && cnt > prevEMA*d.spikeRatio

	if !isSpike {
		st.ema = d.emaAlpha*cnt + (1-d.emaAlpha)*st.ema
	}
	d.mu.Unlock()

	return isSpike
}

func (d *Detector) SetMinDiversity(v float64) {
	d.mu.Lock()
	d.minDiversity = v
	d.mu.Unlock()
}

func (d *Detector) SetSpikeRatio(v float64) {
	d.mu.Lock()
	d.spikeRatio = v
	d.mu.Unlock()
}

func (d *Detector) Prune(active map[uint64]struct{}) {
	d.mu.Lock()
	for h := range d.states {
		if _, ok := active[h]; !ok {
			delete(d.states, h)
		}
	}
	d.mu.Unlock()
}

func (d *Detector) Reset() {
	d.mu.Lock()
	clear(d.states)
	d.mu.Unlock()
}
