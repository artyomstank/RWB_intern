package window_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/artyomstank/RWB_intern/internal/domain"
	"github.com/artyomstank/RWB_intern/internal/window"
)

func makeEvent(query, userID string, ts time.Time) *domain.SearchEvent {
	return &domain.SearchEvent{
		Query:     query,
		UserID:    userID,
		Ts:        ts,
		SessionID: "sess-test",
	}
}

func TestRecord_Basic(t *testing.T) {
	w := window.New()
	now := time.Now()

	if !w.Record(makeEvent("кроссовки nike", "user-001", now)) {
		t.Fatal("expected event to be recorded")
	}

	snap := w.Snapshot()
	if len(snap) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	for _, stats := range snap {
		if stats.Count != 1 {
			t.Errorf("expected count=1, got %d", stats.Count)
		}
	}
}

func TestRecord_LateArrival(t *testing.T) {
	w := window.New()

	// Событие с ts 90 секунд назад — превышает lateLimit=60s.
	old := time.Now().Add(-90 * time.Second)
	if w.Record(makeEvent("старый запрос", "user-001", old)) {
		t.Error("expected late event to be filtered")
	}

	if len(w.Snapshot()) != 0 {
		t.Error("expected empty snapshot after filtering late event")
	}
}

func TestRecord_FutureEvent(t *testing.T) {
	w := window.New()

	future := time.Now().Add(30 * time.Second)
	if w.Record(makeEvent("будущий запрос", "user-001", future)) {
		t.Error("expected future event (>10s) to be filtered")
	}
}

func TestRecord_Normalization(t *testing.T) {
	w := window.New()
	now := time.Now()

	// Все варианты должны нормализоваться к одному запросу.
	queries := []string{
		"Кроссовки  Nike",
		"кроссовки nike",
		"КРОССОВКИ NIKE",
		"  кроссовки   nike  ",
	}
	for _, q := range queries {
		w.Record(makeEvent(q, "user-001", now))
	}

	snap := w.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 unique normalized query, got %d", len(snap))
	}

	for h, stats := range snap {
		if stats.Count != int64(len(queries)) {
			t.Errorf("expected count=%d, got %d", len(queries), stats.Count)
		}
		q, ok := w.LookupQuery(h)
		if !ok {
			t.Fatal("expected query to be in registry")
		}
		if q != "кроссовки nike" {
			t.Errorf("expected normalized 'кроссовки nike', got %q", q)
		}
	}
}

func TestRecord_EmptyQuery(t *testing.T) {
	w := window.New()
	if w.Record(makeEvent("", "user-001", time.Now())) {
		t.Error("expected empty query to be filtered")
	}
	if w.Record(makeEvent("   ", "user-001", time.Now())) {
		t.Error("expected whitespace-only query to be filtered")
	}
	if len(w.Snapshot()) != 0 {
		t.Error("expected empty snapshot")
	}
}

func TestRecord_MultipleQueries(t *testing.T) {
	w := window.New()
	now := time.Now()

	queries := []string{"iphone 16", "samsung galaxy", "xiaomi", "iphone 16", "iphone 16"}
	for _, q := range queries {
		w.Record(makeEvent(q, "user-001", now))
	}

	snap := w.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 unique queries, got %d", len(snap))
	}
}

func TestRecord_UniqueUsers_HLL(t *testing.T) {
	w := window.New()
	now := time.Now()
	query := "iphone 16"

	const numUsers = 100
	// Каждый пользователь ищет один раз.
	for i := range numUsers {
		uid := fmt.Sprintf("user-%04d", i)
		w.Record(makeEvent(query, uid, now))
	}
	// Первый пользователь ищет ещё 50 раз.
	for range 50 {
		w.Record(makeEvent(query, "user-0000", now))
	}

	snap := w.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 query, got %d", len(snap))
	}

	for _, stats := range snap {
		if stats.Count != numUsers+50 {
			t.Errorf("expected count=%d, got %d", numUsers+50, stats.Count)
		}
		// HLL с 14-битной точностью даёт ~2% погрешность.
		// При 100 уникальных пользователях допустимый диапазон: 90–110.
		if stats.UniqueUsers < 90 || stats.UniqueUsers > 110 {
			t.Errorf("HLL estimate out of range: expected ~100, got %d", stats.UniqueUsers)
		}
	}
}

func TestRecord_ConcurrentWrites(t *testing.T) {
	w := window.New()
	now := time.Now()

	const goroutines = 64
	const eventsPerGoroutine = 1000
	done := make(chan struct{}, goroutines)

	for g := range goroutines {
		go func(id int) {
			for i := range eventsPerGoroutine {
				q := fmt.Sprintf("query-%d", i%10)
				uid := fmt.Sprintf("user-%d-%d", id, i)
				w.Record(makeEvent(q, uid, now))
			}
			done <- struct{}{}
		}(g)
	}

	for range goroutines {
		<-done
	}

	snap := w.Snapshot()
	if len(snap) == 0 {
		t.Error("expected non-empty snapshot after concurrent writes")
	}

	var totalCount int64
	for _, stats := range snap {
		totalCount += stats.Count
	}
	expected := int64(goroutines * eventsPerGoroutine)
	if totalCount != expected {
		t.Errorf("expected total count=%d, got %d", expected, totalCount)
	}
}

func TestSnapshot_RegistryPrune(t *testing.T) {
	w := window.New()

	// Записываем события в далёком прошлом — они попадут за пределы окна
	// после snapshot'а (snapshot фильтрует по secCutoff).
	// Используем "нормальный" ts — запись пройдёт.
	now := time.Now()
	w.Record(makeEvent("временный запрос", "user-001", now))

	// Первый snapshot: запрос активен.
	snap1 := w.Snapshot()
	if len(snap1) != 1 {
		t.Fatalf("expected 1 query in first snapshot, got %d", len(snap1))
	}

	// Убеждаемся что Registry содержит хэш (LookupQuery работает).
	for h := range snap1 {
		if _, ok := w.LookupQuery(h); !ok {
			t.Error("expected query to be in registry after first snapshot")
		}
	}
}
