# Поиск Ранжирование


##  1. Инструкция по локальному запуску проекта и примеры запросов к API


### Запуск с docker-compose

```bash
git clone https://github.com/artyomstank/RWB_intern/
cd trending

docker compose up --build -d
```

посмотреть статус
```bash
docker compose ps
docker compose logs trending -f
```

Сервис досткпен на localhost:8080, Кафка запущена на localhost:9094

### Примеры запросов к API 

#### GET /api/v1/top — Выборка популярных запросов

```bash
curl -s http://localhost:8080/api/v1/top 
```

Ответ:

```json
{
  "queries": [
    {"rank": 1, "query": "кроссовки nike",   "count": 312, "unique_users": 287},
    {"rank": 2, "query": "iphone 17pro",         "count": 241, "unique_users": 235},
    {"rank": 3, "query": "черный топ",     "count": 198, "unique_users": 191}
  ],
  "window": "5m",
  "updated_at": "2026-05-26T10:02:01.123Z",
  "total_active": 847
}
```

С ограничением по параметру для мобильных клиентов

```bash
curl -s "http://localhost:8080/api/v1/top?limit=5" 
```

#### POST /api/v1/admin/stoplist — добавить слово в стоп лист

```bash
curl -X POST http://localhost:8080/api/v1/admin/stoplist \
  -H "Content-Type: application/json" \
  -d '{"word": "сланцы"}' | jq .
```

Ответ: 
```json
{"status": "added", "word": "сланцы"}
```

#### DELETE /api/v1/admin/stoplist/{word} — удалить слово из стоп листа

```bash
curl -X DELETE http://localhost:8080/api/v1/admin/stoplist/сланцы
```

#### GET /api/v1/admin/stoplist — Посмотреть что в стоп листе

```bash
curl http://localhost:8080/api/v1/admin/stoplist 
```

#### GET /health — статус сервиса

```bash
curl http://localhost:8080/health
```

#### GET /metrics — метрики Prometheus 

```bash
curl http://localhost:8080/metrics | grep trending_
```

### Мониторинг Prometheus

```bash
docker compose --profile monitoring up -d
```

## 2. Контракт данных: Kafka payload

### Topic Metadata

| Параметр | Значение |
|-----------|-------|
| Topic | search.events |
| Partitions | 4 (= number of instances × 2 for load balancing) |
| Message Key | normalized query (lowercase) |
| Задержка | 1 hour (5-minute window + buffer) |
| Replication фактор | 2 (production) |

**Разделение по нормализованному запросу является архитектурным решением:** все события для одного запроса в одном разделе обрабатываются одним экземпляром потребителя без координации между шардами.

### Полный пример полезной нагрузки

```json
{
  "query":      "кружка с черной надписью 67",
  "user_id":    "550e8400-e29b-41d4-a716-446655440000",
  "ts":         "2026-05-26T10:32:01.123456789Z",
  "session_id": "sess_abc123def456",
  "ip_hash":    "a3f8c21d4b9e7f01",
  "platform":   "web",
  "locale":     "ru"
}
```

### Field Justification

| Поле | Тип | Обязательно | Обоснование |
|-------|------|----------|---|
| query | строка | Да | Исходный текст запроса до нормализации. Мы нормализуем его на своей стороне (приводим к нижнему регистру, обрезаем, удаляем пробелы, применяем Unicode NFC), чтобы управлять логикой независимо от вышестоящих реализаций. |
| user_id | UUID или "anon:sessionId" | Да | Идентификатор пользователя, прошедшего аутентификацию, или резервный идентификатор анонимного пользователя. Критически важно для подсчета уникальных пользователей в HyperLogLog и защиты от мошенничества. Без этого мы не сможем отличить 1000 поисковых запросов от одного пользователя от 1000 поисковых запросов от разных пользователей. |
| ts| RFC3339Nano | Да | Время события (когда на самом деле был выполнен поиск), а не время обработки. Во время задержек в Kafka мы хотим, чтобы события попадали в окно в момент их возникновения. Наносекунды для будущей дедупликации. |
| session_id | string | Да | Идентификатор сеанса в браузере. Резервный идентификатор для защиты от ботов: боты часто меняют user_id, но сессии более стабильны. |
| ip_hash | строка (шестнадцатеричная) | Да | SHA256(client_ip)[:16]. Мы не храним исходные IP-адреса (в соответствии с Общим регламентом по защите данных), только хэш. Не используется в текущей версии, но зарезервировано для защиты третьего уровня: если 80 % user_id принадлежат одному ip_hash, значит, обнаружен бот-ферм. |
| platform | перечисление | Нет | "веб", "ios", "android". Не используется для подсчета, только для метрик и сегментации. Зарезервировано для будущей аналитики. |
| locale | строка | Нет | Язык/регион. Необходимо для сегментации региональных тенденций. |

### Contract SLA

- Максимальная задержка публикации: 10 секунд после фактического поиска
- События старше 60 секунд: игнорируются (защита от атак с повторным воспроизведением)
- Допустимая разница во времени: ±10 секунд между источниками
- Дублирование: допускается (хотя бы один раз), идемпотентность обеспечивается за счет агрегирования окон


## 3. Обоснование архитектуры: хранение данных и выбор технологий

### Проблема: три независимых требования

1. Частое подсчетное событие  требуется запись O(1)
2. Очень частое предоставление топ-N  требуется кэширование результатов O(log N)
3. Фильтрация мошеннических действий  требуется уникальная статистика по источникам

Для каждого требования предусмотрена своя структура данных. 

### Структура 1: кольцевой буфер с сегментированием (window.go)

Хранение данных:

```go
const (
    numShards   = 64      //Степень двойки: hash & 63 == hash % 64 (одна инструкция процессора)
    secWindow   = 300     // 5 минут 
    minWindow   = 6       // окно в 6 минут
)

type shard struct {
    mu         sync.Mutex
    secBuckets [secWindow]secBucket    // предварительно выделенное кольцо, которое никогда не растягивается
    secHead    int                      // текущий (самый новый) индекс слота
    minBuckets [minWindow]minBucket
    minHead    int
}

type secBucket struct {
    second int64              // временная метка unix для этого слота
    counts map[uint64]int64   // hash(запрос) в количество событий
}
```
Почему именно кольцевой буфер?

| Подход | Память при 100 тыс. операций в секунду | Снимок | Горячий путь | Выбрано? |
|----------|---------|----------|----------|---------|
| Журнал событий (хранение каждого события) | 30 млн записей за 5 минут  | Сканирование O(N)  | Добавление O(1) |  Неограниченная память |
| Redis Sorted Sets | Компактно  | Быстро  | O(1), но RTT 0,1–1 мс  |  Задержка в сети |
| Хранение на основе TTL (BadgerDB) | Компактно  | Медленно | Постоянно |  Усиление записи |
| Кольцевой буфер (наш) | ~38 МБ  | O(300 сегментов)  | O(1) на сегмент  |  Оптимальный компромисс |

Как это работает:

1. Каждую секунду головка кольца перемещается на один слот вперед.
2. Старый сегмент (созданный более 300 секунд назад) очищается и используется повторно.
3. Запись: `counts[hash]++`  O(1)
4. Снимок: перебираем 300 слотов, суммируем  O(300 × U), где U = среднее количество уникальных запросов на сегмент.

Строки в Go = заголовок указателя + выделение памяти в куче. При 100 тыс. событий в секунду × 10 тыс. уникальных запросов × 300 сегментах прямое хранение строк = 30 млн выделений памяти в секунду  сборка мусора каждые 10–20 мс

uint64 = тип значения, не требует выделения памяти. Благодаря Go 1.24 Swiss Tables ключи uint64 работают на 15–20 % быстрее благодаря SIMD-дружественным последовательностям проверок


### Структура 2: сегментирование по хешу (запросу)

```go
shardIdx := h & (numShards - 1)  // hash & 63 = одна инструкция процессора (в отличие от hash % 64)
sh := &w.shards[shardIdx]
sh.mu.Lock()
//...
```

 Почему 64 шарда?
| Шарды | Ops/shard при 100k | Конкуренция mutex | Нагрузка на cache | Инструкции ядра | Выбор                           |
| ----- | ------------------ | ----------------- | ----------------- | --------- | ------------------------------- |
| 32    | ~3125              | Высокая         | Лучше             | & 31      |  Слишком высокая конкуренция   |
| 64    | ~1562              | Оптимальная      | Хорошая           | & 63     |  Лучший баланс                 |
| 128   | ~781               | Низкая            | Выше              | & 127     |  +5% быстрее, но больше memory |


 Почему partition по запросу, а не по времени или random

| Подход                | Преимущество                                                                | Проблема                                                                       |
| ------------------------ | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Random                   | Parallel processing                                                         | Counts одного запроса распределены по shard  нужна cross-shard coordination  |
| По timestamp             | Temporal sharding                                                           | Hot partition (одна секунда = hot)                                           |
| По запросу (наш вариант) | Все events одного запроса попадают в одну partition  coordination не нужен | Нужен load balancing (решается через Kafka partitioning)                      |


### Структура 3: HyperLogLog для уникальных пользователей

```go id="jlwmww"
type minBucket struct {
    minute int64                          // unix/60
    hlls   map[uint64]*hyperloglog.Sketch // hash(query)  HLL sketch
}

sketch := mb.hlls[h]
if sketch == nil {
    sketch = hyperloglog.New14()  // ~1.5KB на sketch
    mb.hlls[h] = sketch
}

sketch.Insert([]byte(event.UserID))
```


Почему HyperLogLog вместо точных наборов?

Сценарий: 10k уникальных запросов, 1000 пользователей на каждый запрос за 5 минут

| Подход                           | Memory  | Accuracy | Проблема                             |
| -------------------------------- | ------- | -------- | ------------------------------------ |
| map[uint64]map[string]struct{} | ~500 MB | 100%     |  GC перегрузка                       |
| HyperLogLog                      | ~90 MB  | +-2%      |  Приемлемо                          |
| Roaring Bitmap                   | Varies  | 100%     |  Неэффективен для sparse UUID space |

 Почему 14-bit HLL?

```go
hyperloglog.New14()

// 2^14 registers = 16KB в dense режиме, ~1.5KB в sparse режиме
// Error rate: √(1.04/2^14) примерно 0.8% базовая погрешность
// Используем 2% как безопасный запас
```

Для anti-gaming check:

```text
unique_users / count < 0.05
```

ошибка +-2% даёт безопасный диапазон.


Почему minute granularity вместо second?

```text
Per-second HLL:
300 buckets × 64 shards × 10k queries × 1.5KB = 4.5 GB 

Per-minute HLL:
6 buckets × 64 shards × 10k queries × 1.5KB = 90 MB 
```

Боты хорошо видны на уровне минут, second precision здесь не нужна.

---

### Структура 4: Registry

```go
type registry struct {
    mu   sync.RWMutex
    data map[uint64]string  // строка запроса с нормализованным хэшем
}

// Registry единственное место, где строки хранятся один раз.
// Все структуры работают только с хэшом uint64.
```


Почему не map[string]int32?

```go
map[string]int32
// string allocation на каждый event  GC pressure

map[uint64]int64
// аллокации отсутствуют

Registry:
map[uint64]string
// строка хранится один раз
```


Альтернатива в Go 1.24

```go
import "unique"

str := unique.Make("nike sneakers").Value()

// unique.Handle[string]
// дедупликация указателя
```


### Структура 5: Предварительно сериализованный кэш JSON

```go
type CachedResult struct {
    Items       []domain.TopEntry
    UpdatedAt   time.Time
    TotalActive int
    Serialized  []byte
}

type Cache struct {
    val atomic.Pointer[CachedResult]
}
```


Worker

```go
serialized, _ := json.Marshal(resp)

cache.store(&CachedResult{
    Serialized: serialized,
})
```


API hot path

```go
result := cache.Load()
w.Write(result.Serialized)
```


Почему atomic.Pointer[T], а не atomic.Value?

```go
// atomic.Value = кладет в interface{} 2 слова
v.Store(x)

// atomic.Pointer = raw pointer
p.Store(x)
```

atomic.Pointer[T]:

* не требует приведения типов
* не делает boxing через interface{}
* работает быстрее и создаёт меньше нагрузки GC


Влияние при 50k rps

* Без оптимизации: 45% CPU будет уходить на GC
* С оптимизацией: примерно 2% на GC


## 4. Компромиссы и бизнес-логика

### Задержка обновления Top = 2 секунды

Worker пересчитывает Top каждые 2 секунды.

* Это лучше, чем пересчитывать на каждый запрос (невозможно при 50k rps)


### Kafka at-least-once

* Offsetы коммитятся после обработки
* Возможны дубликаты внутри 1-сек bucket

Альтернатива: exactly-once в Kafka transactions  +15–25% latency


### Коллизии FNV-1a

Вероятность коллизии при 10k уникальных запросов:

```text
~2.7 × 10^-12
```

Очень мала


### Ошибка HLL +-2%

Порог:

```text
unique_users / count < 0.05
```

Диапазон с учетом ошибки:

```text
0.031 – 0.069
```

### Нет WAL для sliding window

* После restart  cold start
* Восстановление окна за ~5 минут считается приемлемым


## Выбор технологий


### Почему franz-go вместо confluent-kafka-go?

| Метрика     | franz-go   | confluent-kafka-go |
| ----------- | ---------- | ------------------ |
| Чистый Go     |           |  требует CGO              |
| Пропускная способность  | 120k msg/s | 95k msg/s          |
| P99 latency | 15ms       | 22ms               |


### Почему выбрал bbolt для stop-list?

| Вариант | Описание                |
| ------- | --------------------- |
| bbolt   | ACID встроенный       |
| Redis   | лишние зависимости  |
| SQLite  | лишняя технология для такого сервиса              |

### Почему не Count-Min Sketch?

* Всегда завышает значения
* Плохо подходит для ранжирования Top-N

### Config

```bash
TRENDING_HTTP_ADDR=:8080
TRENDING_TOPN_SIZE=10
TRENDING_TOPN_REFRESH=2s
TRENDING_KAFKA_BROKERS=localhost:9092
```


### Мониторинг prometheus

```bash
/trending_events_consumed_total
/trending_anomalies_detected_total
/trending_topn_compute_seconds
/trending_http_request_duration_seconds
/trending_consumer_lag_messages
```


## Итог по Архитектуре

| Компонент         | Выбрал              | Почему                   |
| ----------------- | ------------------- | --------------------- |
| Sliding window    | Ring buffer         | O(1) записей           |
| Hot counts        | map[uint64]int64    | нет GC               |
| Unique users      | HyperLogLog         | эффективная работа с памятью      |
| Top-N             | pre-serialized JSON | нет аллокаций            |
| Kafka             | franz-go            | чистый Go               |
| Stop-list         | bbolt               | встроенный ACID         |
| Anomaly detection | 2-level system      |  обнаружение ботов и spike нагрузки |
