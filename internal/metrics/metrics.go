package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics содержит все счётчики и гистограммы сервиса.
type Metrics struct {
	// EventsConsumed — успешно обработанные события из Kafka.
	EventsConsumed prometheus.Counter

	// EventsDropped — события, которые не удалось распарсить.
	EventsDropped prometheus.Counter

	// EventsFiltered — события, отфильтрованные из-за позднего прихода или будущего ts.
	EventsFiltered prometheus.Counter

	// AnomaliesDetected — запросы, исключённые как аномалии (накрутка).
	AnomaliesDetected prometheus.Counter

	// TopNComputeTime — время вычисления топа фоновым воркером.
	TopNComputeTime prometheus.Histogram

	// HTTPDuration — латентность HTTP-запросов (по методу, пути, статусу).
	HTTPDuration *prometheus.HistogramVec

	// ConsumerLag — отставание консьюмера от последнего офсета в Kafka.
	ConsumerLag prometheus.Gauge

	// StopListSize — текущий размер стоп-листа.
	StopListSize prometheus.Gauge

	// TopNLastUpdated — UNIX timestamp последнего обновления топа.
	// Используется для детектирования зависшего воркера.
	TopNLastUpdated prometheus.Gauge
}

// New создаёт и регистрирует все метрики в переданном Registerer.
func New(reg prometheus.Registerer) *Metrics {
	eventsConsumed := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trending_events_consumed_total",
		Help: "Total number of search events successfully consumed from Kafka.",
	})
	eventsDropped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trending_events_dropped_total",
		Help: "Total number of events dropped due to parse or validation errors.",
	})
	eventsFiltered := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trending_events_filtered_total",
		Help: "Total number of events filtered out (late arrival or future timestamp).",
	})
	anomaliesDetected := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "trending_anomalies_detected_total",
		Help: "Total number of queries excluded as anomalies (bot traffic).",
	})
	topNComputeTime := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "trending_topn_compute_seconds",
		Help:    "Time spent computing the top-N result in the background worker.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})
	httpDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "trending_http_request_duration_seconds",
		Help:    "HTTP request latency by method, matched route pattern, and status code.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	}, []string{"method", "pattern", "status"})
	consumerLag := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trending_consumer_lag_messages",
		Help: "Estimated Kafka consumer lag in number of messages.",
	})
	stopListSize := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trending_stoplist_size",
		Help: "Current number of words in the stop list.",
	})

	topNLastUpdated := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "trending_topn_last_updated_timestamp",
		Help: "UNIX timestamp of the last top-N update. Helps detect stalled workers.",
	})

	reg.MustRegister(
		eventsConsumed,
		eventsDropped,
		eventsFiltered,
		anomaliesDetected,
		topNComputeTime,
		httpDuration,
		consumerLag,
		stopListSize,
		topNLastUpdated,
	)

	return &Metrics{
		EventsConsumed:    eventsConsumed,
		EventsDropped:     eventsDropped,
		EventsFiltered:    eventsFiltered,
		AnomaliesDetected: anomaliesDetected,
		TopNComputeTime:   topNComputeTime,
		HTTPDuration:      httpDuration,
		ConsumerLag:       consumerLag,
		StopListSize:      stopListSize,
		TopNLastUpdated:   topNLastUpdated,
	}
}
