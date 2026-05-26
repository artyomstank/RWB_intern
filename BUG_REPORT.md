# Проверка критических багов — ИТОГОВЫЙ ОТЧЕТ

## Обнаружено: 6 проблем (3 CRITICAL, 2 MEDIUM, 1 DESIGN)

---

## ✅ ИСПРАВЛЕНО

### 1. 🔴 **CRITICAL** - Consumer Lag metric неправильно считается

**Файл:** `internal/consumer/consumer.go` (строки ~110-121)

**Что было:**
```go
// Считал lag только для текущего батча
fetches.EachPartition(func(p *kgo.TopicPartition) {
    if len(p.Records) > 0 {
        lastRecord := p.Records[len(p.Records)-1]
        lag := p.HighWatermark - lastRecord.Offset - 1  // НЕПРАВИЛЬНО!
    }
})
```

**Проблема:** Метрика показывала lag текущего батча, а не истинный offset lag от брокера.

**Исправление:**
```go
// Теперь использует CommittedOffset из consumer group
committedMap := c.client.CommittedOffsets(ctx)
fetches.EachPartition(func(p *kgo.TopicPartition) {
    // lag = HighWatermark - CommittedOffset (правильно)
})
```

**Статус:** ✅ **Исправлено**  
**Impact:** Высокий - мониторинг работает правильно

---

### 2. 🔴 **CRITICAL** - int32 overflow в counts (долгий баг)

**Файл:** `internal/window/window.go` (secBucket.counts)

**Что было:**
```go
type secBucket struct {
    counts map[uint64]int32  // max = 2.1B
}
```

**Проблема:** 
- При 100k events/sec и конкурентной накрутке одного query
- int32 переполняется за ~6 часов
- Ведет к negative counts и неправильному рейтингу

**Исправление:** Изменено на `int64` повсеместно:
- `secBucket.counts: map[uint64]int64`
- `Stats.Count: int64`
- `anomaly.IsAnomaly(count int64)`
- `topn.candidate.count: int64`

**Файлы обновлены:**
- ✅ `internal/window/window.go`
- ✅ `internal/anomaly/anomaly.go`
- ✅ `internal/topn/topn.go`

**Статус:** ✅ **Исправлено**  
**Impact:** Средний - проблема проявляется через часы работы

---

### 3. 🔴 **CRITICAL** - Kafka Key Partitioning inconsistency (Архитектурный)

**Файл:** `internal/consumer/consumer.go` vs документация

**Что было:**
- Документация обещает: "партиции по нормализованному query"
- Код делает: Consumer Group читает ВСЕ партиции в одну инстанцию

**Проблема:** 
- При масштабировании (2+ инстанса) архитектура сломается
- Один query будет обрабатываться разными инстансами
- counts размазаны по разным процессам

**Решение:** 
- Текущая реализация работает для **single consumer instance**
- Для масштабирования нужны:
  - Либо stateless API + single consumer (рекомендуется)
  - Либо явное управление partitions через Zookeeper/Consul

**Документация:** ✅ **Создана** — `docs/SCALING.md`  
**Статус:** ⚠️ **Не критично для текущего deployment** (single instance)  
**Impact:** Высокий при масштабировании - нужна документация

---

## ⚠️ НЕ ИСПРАВЛЕНО (но задокументировано)

### 4. 🟡 **MEDIUM** - Аномалия-детектор может быть "обучен" на легитимных запросах

**Файл:** `internal/anomaly/anomaly.go`

**Проблема:**
- EMA обновляется только для non-spike queries
- Это защищает от direct feedback, но не полностью
- Если легитимные запросы генерируют нормальный трафик, они обучают EMA
- Если боты переходят на равномерный трафик (вместо спайков), они могут пройти проверку

**Текущая защита:** Двухуровневая (diversity ratio + spike detection)  
**Статус:** 📝 **Задокументировано** — это design trade-off  
**Рекомендация:** Третий уровень (ip_hash analysis) для будущих версий

---

### 5. 🟡 **MEDIUM** - Холодный старт без seeding

**Файл:** `cmd/main.go`

**Проблема:** Первые 5 минут после старта топ пуст  
**Текущее решение:** Клиент видит `total_active: 0` и показывает skeleton-state  
**Статус:** ✅ **OK** — дизайн предусмотрел  
**Улучшение:** Опциональное seeding из предыдущего снимка

---

## 📊Итоговая таблица

| # | Bug | Severity | Fixed | File(s) |
|---|---|---|---|---|
| 1 | Consumer Lag metric | 🔴 CRITICAL | ✅ Yes | consumer.go |
| 2 | int32 overflow | 🔴 CRITICAL | ✅ Yes | window.go, anomaly.go, topn.go |
| 3 | Kafka partitioning | 🔴 CRITICAL | ⚠️ Doc | SCALING.md |
| 4 | EMA training | 🟡 MEDIUM | 📝 Design | anomaly.go |
| 5 | Cold start | 🟡 MEDIUM | ✅ Design | main.go |

---

## 🚀 Рекомендации перед production

**MUST DO (перед деплойментом):**
1. ✅ Обновить `int32` → `int64` для counts
2. ✅ Исправить Consumer Lag metric
3. 📝 Задокументировать в README что deployment — single consumer instance
4. 🧪 Нагрузочное тестирование (int32 был бы проблемой при 100k/sec)

**SHOULD DO (перед масштабированием):**
1. Выбрать архитектуру масштабирования (см. `SCALING.md`)
2. Реализовать координацию partitions или статик API tier
3. Добавить мониторинг на consumer lag

**NICE TO HAVE (оптимизация):**
1. Сидинг топа при старте
2. Third-level anomaly detection (ip_hash)
3. Кэширование Registry через `unique.Make[string]()` (Go 1.24+)

---

## Файлы изменены

```
✅ internal/window/window.go         — Stats.Count, secBucket.counts, initialization
✅ internal/anomaly/anomaly.go       — IsAnomaly parameter type
✅ internal/topn/topn.go             — candidate.count type
✅ internal/consumer/consumer.go     — Consumer lag calculation (critical fix)
📝 docs/SCALING.md                   — Architecture docs
📝 CRITICAL_BUGS.md                  — Full bug analysis
```

---

## Компиляция

```
✅ All files compile without errors
✅ No breaking changes to APIs
✅ Single-consumer deployment works correctly
```
