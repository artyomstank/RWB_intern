// Package stoplist реализует динамический стоп-лист поисковых запросов.
//
// Хранение: bbolt (embedded B-tree KV) для персистентности + sync.RWMutex map
// для быстрого Contains() в hot path фонового воркера.
//
// bbolt — embedded, ACID (WAL), без внешних зависимостей.
// Единственный минус: один writer за раз, — но Add/Remove вызываются редко.
package stoplist

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/text/unicode/norm"
)

var bucketName = []byte("stoplist")

// Entry — одна запись стоп-листа.
type Entry struct {
	Word    string    `json:"word"`
	AddedAt time.Time `json:"added_at"`
}

// StopList — основная структура.
type StopList struct {
	db    *bolt.DB
	mu    sync.RWMutex
	words map[string]Entry // in-memory кэш для O(1) Contains()
}

// New открывает или создаёт bbolt БД и загружает слова в память.
func New(path string) (*StopList, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}

	sl := &StopList{
		db:    db,
		words: make(map[string]Entry),
	}

	// Инициализируем бакет и загружаем существующие слова.
	err = db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return bkt.ForEach(func(k, v []byte) error {
			var e Entry
			if jsonErr := json.Unmarshal(v, &e); jsonErr != nil {
				// Пропускаем повреждённые записи, не прерываем загрузку.
				return nil
			}
			sl.words[string(k)] = e
			return nil
		})
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return sl, nil
}

// Contains возвращает true если запрос (целиком) есть в стоп-листе.
// Вызывается из горячего пути фонового воркера — только RLock.
// Сравнение точное (нормализованная строка), не substring.
func (sl *StopList) Contains(word string) bool {
	sl.mu.RLock()
	_, ok := sl.words[word]
	sl.mu.RUnlock()
	return ok
}

// Add добавляет слово в стоп-лист (персистентно + in-memory).
// Идемпотентно: повторное добавление обновляет AddedAt.
// Слово нормализуется перед сохранением для согласованности с window.
func (sl *StopList) Add(word string) error {
	word = normalizeWord(word)
	if word == "" {
		return nil // пустая строка после нормализации — пропускаем
	}

	e := Entry{Word: word, AddedAt: time.Now().UTC()}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}

	if err := sl.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(word), data)
	}); err != nil {
		return err
	}

	sl.mu.Lock()
	sl.words[word] = e
	sl.mu.Unlock()
	return nil
}

// Remove удаляет слово из стоп-листа.
// Если слова нет — не ошибка (идемпотентно).
// Слово нормализуется перед удалением для согласованности с window.
func (sl *StopList) Remove(word string) error {
	word = normalizeWord(word)
	if word == "" {
		return nil // пустая строка после нормализации — пропускаем
	}

	if err := sl.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(word))
	}); err != nil {
		return err
	}

	sl.mu.Lock()
	delete(sl.words, word)
	sl.mu.Unlock()
	return nil
}

// List возвращает все слова стоп-листа, отсортированные по времени добавления.
func (sl *StopList) List() []Entry {
	sl.mu.RLock()
	result := make([]Entry, 0, len(sl.words))
	for _, e := range sl.words {
		result = append(result, e)
	}
	sl.mu.RUnlock()

	slices.SortFunc(result, func(a, b Entry) int {
		return a.AddedAt.Compare(b.AddedAt)
	})
	return result
}

// Close закрывает bbolt БД.
func (sl *StopList) Close() error {
	return sl.db.Close()
}

// normalizeWord нормализует слово аналогично window.Record().
func normalizeWord(s string) string {
	if s == "" {
		return ""
	}
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}
