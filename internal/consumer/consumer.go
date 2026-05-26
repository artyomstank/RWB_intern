// Package consumer реализует Kafka-консьюмер на базе franz-go.
//
// Выбор franz-go над confluent-kafka-go:
//   - Pure Go, нет зависимости от librdkafka (CGO-free).
//   - Лучший throughput в бенчмарках Go-экосистемы.
//   - Нативная поддержка Go-контекста и errgroup.
//
// Семантика доставки: at-least-once.
// Офсеты коммитятся после обработки каждого батча.
// При краше между обработкой и коммитом батч будет обработан повторно —
// это допустимо: агрегация по кольцевому буферу идемпотентна для дублей.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/artyomstank/RWB_intern/internal/domain"
	"github.com/artyomstank/RWB_intern/internal/metrics"
	"github.com/artyomstank/RWB_intern/internal/window"
)

// Consumer читает события из Kafka и передаёт их в Window.
type Consumer struct {
	client *kgo.Client
	win    *window.Window
	m      *metrics.Metrics
}

// New создаёт и подключает Kafka-консьюмер.
func New(
	brokers []string,
	topic,
	group string,
	win *window.Window,
	m *metrics.Metrics,
) (*Consumer, error) {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),

		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),

		// Ручной commit offsets
		kgo.DisableAutoCommit(),

		// Защита от ребалансировки во время обработки batch
		kgo.BlockRebalanceOnPoll(),

		// batching
		kgo.FetchMaxBytes(5*1024*1024),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(500*time.Millisecond),

		// читать только новые сообщения
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client: client,
		win:    win,
		m:      m,
	}, nil
}

// Run запускает poll-loop.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)

		if fetches.IsClientClosed() {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Ошибки отдельных partition
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error(
				"kafka fetch error",
				"topic", topic,
				"partition", partition,
				"err", err,
			)
		})

		var consumed, dropped, filtered int

		// Обработка records
		fetches.EachRecord(func(r *kgo.Record) {
			ok, isFiltered := c.handleRecord(r)

			switch {
			case ok:
				consumed++

			case isFiltered:
				filtered++

			default:
				dropped++
			}
		})

		// =========================================================
		// COMMIT OFFSETS
		// =========================================================
		//
		// CommitUncommittedOffsets:
		// - коммитит offsets всех успешно fetched records
		// - используется вместе с DisableAutoCommit()
		// - сохраняет at-least-once semantics
		//
		// Если приложение упадёт:
		//   обработка выполнится повторно
		// что допустимо для идемпотентной агрегации.
		//
		// =========================================================

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error(
				"failed to commit kafka offsets",
				"err", err,
			)
		}

		// Метрики
		if consumed > 0 || dropped > 0 || filtered > 0 {
			c.m.EventsConsumed.Add(float64(consumed))
			c.m.EventsDropped.Add(float64(dropped))
			c.m.EventsFiltered.Add(float64(filtered))
		}

		// =========================================================
		// CONSUMER LAG
		// =========================================================

		var totalLag int64

		committedMap := c.client.CommittedOffsets()

		if committedMap != nil {
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {

				topicOffsets, ok := committedMap[p.Topic]
				if !ok {
					return
				}

				committedOffset, ok := topicOffsets[p.Partition]
				if !ok {
					committedOffset = kgo.EpochOffset{
						Offset: -1,
					}
				}

				// lag = HWM - committed - 1
				if committedOffset.Offset >= 0 &&
					p.HighWatermark > committedOffset.Offset {

					lag := p.HighWatermark -
						committedOffset.Offset - 1

					if lag > 0 {
						totalLag += lag
					}
				}
			})
		}

		c.m.ConsumerLag.Set(float64(totalLag))
	}
}

// handleRecord парсит одно Kafka-сообщение.
// Возвращает:
//
//	recorded, filtered
func (c *Consumer) handleRecord(
	r *kgo.Record,
) (recorded, filtered bool) {

	var event domain.SearchEvent

	if err := json.Unmarshal(r.Value, &event); err != nil {
		slog.Debug(
			"consumer: failed to parse event",
			"offset", r.Offset,
			"partition", r.Partition,
			"err", err,
		)

		return false, false // dropped
	}

	if event.Query == "" || event.UserID == "" {
		slog.Debug(
			"consumer: skipping invalid event",
			"offset", r.Offset,
		)

		return false, false
	}

	if !c.win.Record(&event) {
		return false, true // filtered
	}

	return true, false
}

// Close закрывает Kafka-клиент.
func (c *Consumer) Close() {
	c.client.Close()
}
