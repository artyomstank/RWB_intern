// Package anomaly реализует двухуровневую защиту от накрутки поисковых трендов.
//
// Уровень 1 (diversity ratio): если unique_users / count < minDiversity,
// запрос исходит от слишком малого числа пользователей относительно объёма —
// признак бота. Ботам сложно обеспечить разнообразие user_id при большом объёме.
//
// Уровень 2 (velocity spike): если count > EMA * spikeRatio,
// произошёл аномальный всплеск активности. Легитимный новостной тренд
// обычно проходит уровень 1 (высокий diversity), поэтому до уровня 2 доходит реже.
package anomaly

import (
	"sync"
)

const (
	// minCountForCheck — минимальное число событий для проверки аномалии.
	// Очень редкие запросы не проверяем: слишком мало данных для статистики.
	minCountForCheck = 10

	defaultMinDiversity = 0.05 // unique_users/count < 5% → аномалия
	defaultEMAAlpha     = 0.15 // сглаживающий коэффициент EMA
	defaultSpikeRatio   = 8.0  // count > ema×8 → аномальный всплеск
)

type queryState struct {
	ema float64
}

// Detector — stateful детектор аномалий.
// Хранит EMA для каждого запроса; обновляется при каждом вызове IsAnomaly.
type Detector struct {
	mu           sync.Mutex
	states       map[uint64]*queryState
	minDiversity float64
	emaAlpha     float64
	spikeRatio   float64
}

// New создаёт Detector с дефолтными параметрами.
func New() *Detector {
	return &Detector{
		states:       make(map[uint64]*queryState),
		minDiversity: defaultMinDiversity,
		emaAlpha:     defaultEMAAlpha,
		spikeRatio:   defaultSpikeRatio,
	}
}

// IsAnomaly проверяет, является ли запрос аномальным.
// EMA обновляется только для запросов, прошедших оба фильтра — иначе
// бот может постепенно "обучить" детектор принимать нарастающий трафик.
func (d *Detector) IsAnomaly(h uint64, count int64, uniqueUsers uint64) bool {
	if count < minCountForCheck {
		return false
	}

	cnt := float64(count)
	uniq := float64(uniqueUsers)

	// Уровень 1: diversity ratio.
	// HLL даёт ±2% погрешность, порог 0.05 выбран с запасом.
	if uniq/cnt < d.minDiversity {
		return true
	}

	// Уровень 2: EMA velocity spike.
	d.mu.Lock()
	st, ok := d.states[h]
	if !ok {
		// Первое появление запроса — инициализируем EMA текущим значением.
		d.states[h] = &queryState{ema: cnt}
		d.mu.Unlock()
		return false
	}

	prevEMA := st.ema
	isSpike := prevEMA > 0 && cnt > prevEMA*d.spikeRatio

	if !isSpike {
		// Обновляем EMA только если это НЕ аномалия.
		st.ema = d.emaAlpha*cnt + (1-d.emaAlpha)*st.ema
	}
	d.mu.Unlock()

	return isSpike
}

// SetMinDiversity динамически обновляет порог diversity ratio.
func (d *Detector) SetMinDiversity(v float64) {
	d.mu.Lock()
	d.minDiversity = v
	d.mu.Unlock()
}

// SetSpikeRatio динамически обновляет порог velocity spike.
func (d *Detector) SetSpikeRatio(v float64) {
	d.mu.Lock()
	d.spikeRatio = v
	d.mu.Unlock()
}

// Prune удаляет EMA-состояния для запросов, вышедших из скользящего окна.
// Вызывается фоновым воркером после каждого Snapshot.
func (d *Detector) Prune(active map[uint64]struct{}) {
	d.mu.Lock()
	for h := range d.states {
		if _, ok := active[h]; !ok {
			delete(d.states, h)
		}
	}
	d.mu.Unlock()
}

// Reset сбрасывает накопленное состояние EMA (для тестирования).
func (d *Detector) Reset() {
	d.mu.Lock()
	clear(d.states)
	d.mu.Unlock()
}
