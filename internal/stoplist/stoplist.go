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

type Entry struct {
	Word    string    `json:"word"`
	AddedAt time.Time `json:"added_at"`
}

type StopList struct {
	db    *bolt.DB
	mu    sync.RWMutex
	words map[string]Entry
}

func New(path string) (*StopList, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}

	sl := &StopList{
		db:    db,
		words: make(map[string]Entry),
	}

	err = db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return bkt.ForEach(func(k, v []byte) error {
			var e Entry
			if jsonErr := json.Unmarshal(v, &e); jsonErr != nil {
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

func (sl *StopList) Contains(word string) bool {
	sl.mu.RLock()
	_, ok := sl.words[word]
	sl.mu.RUnlock()
	return ok
}

func (sl *StopList) Add(word string) error {
	word = normalizeWord(word)
	if word == "" {
		return nil
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

func (sl *StopList) Remove(word string) error {
	word = normalizeWord(word)
	if word == "" {
		return nil
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

func (sl *StopList) Close() error {
	return sl.db.Close()
}

func normalizeWord(s string) string {
	if s == "" {
		return ""
	}
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}
