# Критические баги в проекте

## 🔴 БАГ 1: Неправильный расчет Consumer Lag (CRITICAL)

**Файл:** `internal/consumer/consumer.go`  
**Строки:** ~110-121

**Проблема:**
```go
// НЕПРАВИЛЬНО:
var totalLag int64
fetches.EachPartition(func(p *kgo.TopicPartition) {
    if len(p.Records) > 0 {
        lastRecord := p.Records[len(p.Records)-1]
        if p.HighWatermark > lastRecord.Offset {
            lag := p.HighWatermark - lastRecord.Offset - 1
            totalLag += lag
        }
    }
})
```

Это считает lag только для текущего PollFetches батча, а не истинный consumer lag!
- `lastRecord.Offset` — это последний офсет в текущем батче, может быть произвольный
- `HighWatermark - lastRecord.Offset` — это не lag
- Истинный lag = `HighWatermark - CommittedOffset` для каждой партиции

**Последствие:** Метрика `trending_consumer_lag_messages` неправильная, Prometheus/мониторинг не заметит реального отставания консьюмера от брокера.

**Решение:**
```go
// Правильно:
var totalLag int64
lags := c.client.CommittedOffsets(ctx, c.client.AssignedPartitions()...)
for _, partition := range c.client.AssignedPartitions() {
    committed := int64(-1)
    if offsets, ok := lags[partition.Topic]; ok {
        if off, ok := offsets[partition.Partition]; ok {
            committed = off.Offset
        }
    }
    // Получить HighWatermark для партиции через GetOffsets или из Metadata
    // lag = hwm - committed
    totalLag += (hwm - committed)
}
c.m.ConsumerLag.Set(float64(totalLag))
```

---

## 🔴 БАГ 2: Несоответствие контракта Kafka Key (CRITICAL - только при масштабировании)

**Файл:** документация (контракт) vs `internal/consumer/consumer.go`  
**Проблема:**

Документация говорит:
> "Партиционирование по нормализованному запросу — это архитектурное решение, а не деталь. Все события одного запроса попадают в одну партицию, и при горизонтальном масштабировании сервиса каждый инстанс обрабатывает непересекающееся подмножество запросов без необходимости координации."

Но в коде:
- Consumer просто читает все события из всех партиций `kgo.ConsumeTopics(topic)` 
- Нет явного использования Key для партиционирования
- При горизонтальном масштабировании (несколько инстансов Consumer) это сломает идею "каждый инстанс обрабатывает подмножество":
  - Если инстанс-1 и инстанс-2 оба читают ВСЕ события, то counts одного query дублируются
  - Если используется Group, то события распределяются случайно между инстансами, одного query могут обработать разные инстансы
  - **Итог:** counts одного query размазаны по разным шардам в разных инстансах → невозможно суммировать правильно

**Последствие:** При добавлении второго инстанса сервиса:
- Два инстанса обрабатывают одни и те же события
- counts удваиваются, ranking сломан
- Или распределяются случайно — одно и то же еще хуже

**Решение:** 
Либо:
1. Каждый инстанс должен читать определённый набор партиций на основе Key (нужна координация)
2. Или один инстанс консьюмера на несколько инстансов API (сепарировать consumer tier от compute tier)
3. Или признать что горизонтальное масштабирование только на API tier, consumer остается одним

**Текущий код это не поддерживает явно.**

---

## 🔴 БАГ 3: int32 overflow в secBucket counts (Slow but Critical)

**Файл:** `internal/window/window.go`  
**Тип:** `map[uint64]int32` в secBucket

**Проблема:**
```go
type secBucket struct {
    second int64            
    counts map[uint64]int32  // ← int32, max = 2,147,483,647
}

// В Record:
sh.secBuckets[sh.secHead].counts[h]++  // ← При 100k queries/sec
```

- int32 max = 2^31 - 1 = 2.1 миллиарда
- При 100k событий/сек в один секундный бакет: 100k in 1 sec ✓ (в пределах int32)
- Но при многих queries одного типа за сутки:
  - 1 query, 1 событие в сек × 86400 секунд = 86400 (ok)
  - 1 query, 100 событий в сек × 86400 = 8.64 млн (ok)
  - 1 query, 10000 событий в сек × 21600 сек = 216 млн (ok за 6 часов)
  - При конкурентной накрутке одного query можно достичь overflow за часы

**Последствие:** int32 переполняется → negative counts → неправильный ранкинг в топе

**Решение:** Изменить на `int64`:
```go
type secBucket struct {
    second int64
    counts map[uint64]int64  // ← int64
}
```

---

## 🟡 БАГ 4: Нет защиты от race condition при горизонтальном масштабировании Registry (Medium - только при масштабировании)

**Файл:** `internal/window/window.go`  
**Проблема:**

Если в будущем запустят несколько инстансов window.Window (распределённый window), Registry не потерпит конкурентный доступ из разных goroutines разных процессов.

На самом деле это не баг текущей реализации (она single-process), но архитектурная уязвимость для масштабирования.

**Текущий статус:** OK для single-process, но документировать нужно.

---

## 🟡 БАГ 5: Аномалии-детектор может быть "обучен" бот-ом за счет других запросов (Design issue, Medium)

**Файл:** `internal/anomaly/anomaly.go`  
**Проблема:**

Текущая логика:
```go
if !isSpike {
    st.ema = d.emaAlpha*cnt + (1-d.emaAlpha)*st.ema  // Update only for non-anomalies
}
```

Это исправляет direct feedback loop, но не решает проблему полностью:
- Если есть 100 queries и 10 из них — боты, которые создают спайки
- EMA для остальных 90 queries обновляется нормально
- Но если все 10 бот-queries заблокированы spikes, они никогда не обучают EMA
- А легитимные запросы обучают EMA
- **Результат:** Если боты меняют тактику с спайков на равномерный трафик (100 events/sec вместо 10000 за раз), они пройдут через spike-detector, т.к. EMA был обучен на нормальном трафике

**Степень критичности:** Medium - это design trade-off, документировано в задаче. Требуется второй уровень защиты (diversity ratio), который существует.

---

## 🟡 БАГ 6: Холодный старт без seeding (Design, Medium)

**Файл:** `cmd/main.go`, `internal/topn/topn.go`  
**Проблема:**

При первом запуске сервиса cache пуст 5 минут. Виджет на фронте покажет пустой топ.

**Текущее поведение:** Явное поле `total_active: 0` позволяет клиенту показать skeleton-state. Это OK.

**Потенциальное улучшение:** Seeding из снимка топа из БД или соседнего инстанса, но это за рамками задачи.

---

## 🟢 Потенциальные проблемы (не критичные, но стоит знать):

### 1. FNV-1a vs maphash — неполное решение
- FNV заменили на maphash (хорошо)
- Но в registry.prune() используется фиксированный seed (хорошо для консистентности)
- ✓ Ok

### 2. HLL.Merge() может быть дорогим
- Каждый снапшот мержит до 6 HLL-скетчей для каждого query
- При 10k queries это 60k операций Merge
- **Это ok, Merge в HLL O(1) по памяти, но O(K) по времени, где K = precision parameter
- ✓ Ok

### 3. Нет timeout на Snapshot()
- Если Snapshot() зависнет на каком-то шарде (deadlock), весь topn-worker встанет
- Нет очевидного места для deadlock, но стоит добавить timeout для safety
- ⚠️ Minor

### 4. EMA параметры не адаптивные
- defaultEMAAlpha = 0.15 — жестко закодирован
- Можно менять через SetSpikeRatio/SetMinDiversity, но не EMAAlpha
- ⚠️ Nice-to-have

---

## Приоритет исправлений:

1. **CRITICAL - должны быть исправлены немедленно:**
   - БАГ 1: Consumer Lag metric
   - БАГ 2: Kafka Key partitioning (если есть планы масштабирования)

2. **IMPORTANT - исправить до production:**
   - БАГ 3: int32 overflow

3. **MEDIUM - стоит улучшить:**
   - БАГ 4: Race condition protection (если будет масштабирование)
   - БАГ 5: Anomaly detector design (документировать ограничения)

4. **LOW - nice-to-have:**
   - БАГ 6: Холодный старт
   - Добавить timeout на Snapshot()
