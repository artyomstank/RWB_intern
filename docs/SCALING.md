# Архитектурные решения для масштабирования

## Kafka Consumer Group и partitioning

### Текущая реализация (Single Consumer Instance)

```go
kgo.ConsumerGroup(group)  // Joinит consumer group
kgo.ConsumeTopics(topic)  // Читает все партиции в group
```

**Как это работает:**
- Один инстанс сервиса имеет один consumer client
- Consumer group автоматически assign-ет все партиции одному consumer-у
- Все события из всех партиций обрабатываются единственным инстансом
- Sharding по hash(query) внутри одного инстанса гарантирует что один query всегда в одном шарде

**Требования:**
- Consumer group должна иметь rebalance.timeout достаточно большой
- BlockRebalanceOnPoll() предотвращает обработку одного события дважды

---

## Масштабирование на несколько инстансов

### Проблема:
Если запустить 2+ инстанса с одним consumer group:
- Kafka autoматически распределит партиции между инстансами
- Например, инстанс-1 получит partition-0,1  инстанс-2 получит partition-2,3
- Но sharding по hash(query) остается local в каждом инстансе:
  - query="кроссовки" может хэшироваться в shard-5 в инстанс-1
  - query="кроссовки" может хэшироваться в shard-20 в инстанс-2
  - (Обе инстанса используют одинаковый globalSeed в maphash, поэтому один query всегда одинаковый hash внутри процесса)
  
**Итог:** counts одного query размазаны по разным инстансам → невозможно правильно суммировать!

### Решение 1: Stateless API tier + Centralized Consumer

```
[Kafka] → [Consumer (single instance)]
            ↓
        [Window + TopN Cache]
         ↙    ↓    ↘
    [API-1] [API-2] [API-3]
```

- Один consumer обрабатывает все события
- Несколько stateless API инстансов читают из shared cache (через Redis или shared memory)
- **Требует:** Координация доступа к Window/Cache (redis или gRPC)

### Решение 2: Consumer Group с явным управлением партициями

```go
// Инстанс-1:
assignments := map[int32]kgo.EpochOffset{0: {Epoch: -1, Offset: 0}, 1: {}}
c.client.PartitionsToOffsets(ctx, map[string][]int32{
    "search.events": {0, 1},  // Явно assign partitions 0 и 1
})

// Инстанс-2:
assignments := map[int32]kgo.EpochOffset{2: {}, 3: {}}
c.client.PartitionsToOffsets(ctx, map[string][]int32{
    "search.events": {2, 3},  // Явно assign partitions 2 и 3
})
```

- Каждый инстанс обрабатывает фиксированное подмножество партиций
- Для этого нужна координация (Consul, etcd, Zookeeper)
- **Требует:** 
  - Zookeeper или Consul для координации
  - Custom partition assignment logic
  - Graceful rebalancing при добавлении/удалении инстансов

### Решение 3: Hash Ring (Consistent Hashing)

```
query → hash → ring position → assign to instance-N

[Instance-1] [Instance-2] [Instance-3]
  queries:      queries:       queries:
  0-1000       1000-2000     2000-3000
```

- Каждый query маршрутится в определённый инстанс на основе хэша
- Очень сложно и требует coordination с kafka consumer group
- **Требует:** LoadBalancer с consistent hashing

---

## Рекомендация для текущего проекта

**Текущая реализация подходит для:**
- Single consumer instance
- Несколько stateless API instances
- Cache распределяется через HTTP (GET /api/v1/top)

**Если нужно масштабировать consumer:**
1. Использовать **Решение 1** (Stateless API + Single Consumer)
   - Проще всего, минимум изменений
   - Consumer остаётся в отдельном process/pod
   - API instances читают топ через /metrics или shared memory

2. Или переписать на **Решение 2** с Zookeeper
   - Более сложно, но полностью горизонтально масштабируемо
   - Каждый инстанс имеет свой Window
   - Требует merging результатов топ-N от всех инстансов (SQL-style)

**Текущий выбор:** Implicit Single Consumer (Решение 1 без explicit separation)
