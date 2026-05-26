package stoplist_test

import (
	"os"
	"testing"
	"time"

	"github.com/artyomstank/RWB_intern/internal/stoplist"
)

func newTempStopList(t *testing.T) (*stoplist.StopList, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "stoplist-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	sl, err := stoplist.New(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}

	cleanup := func() {
		sl.Close()
		os.Remove(f.Name())
	}
	return sl, cleanup
}

func TestStopList_EmptyByDefault(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	if sl.Contains("кеды") {
		t.Error("expected empty stoplist on startup")
	}
	if len(sl.List()) != 0 {
		t.Error("expected empty list on startup")
	}
}

func TestStopList_Add(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	if err := sl.Add("кеды"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if !sl.Contains("кеды") {
		t.Error("expected 'кеды' to be in stoplist after Add")
	}
}

func TestStopList_Add_Idempotent(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	if err := sl.Add("сланцы"); err != nil {
		t.Fatal(err)
	}
	if err := sl.Add("сланцы"); err != nil {
		t.Fatalf("second Add should be idempotent, got error: %v", err)
	}

	list := sl.List()
	if len(list) != 1 {
		t.Errorf("expected 1 entry after idempotent add, got %d", len(list))
	}
}

func TestStopList_Remove(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	sl.Add("нежелательное слово")
	if err := sl.Remove("нежелательное слово"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if sl.Contains("нежелательное слово") {
		t.Error("expected word to be removed from stoplist")
	}
}

func TestStopList_Remove_Idempotent(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	if err := sl.Remove("несуществующее"); err != nil {
		t.Errorf("Remove of non-existent word should be idempotent, got: %v", err)
	}
}

func TestStopList_List_SortedByTime(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	words := []string{"слово-а", "слово-б", "слово-в"}
	for _, w := range words {
		sl.Add(w)
		time.Sleep(2 * time.Millisecond)
	}

	list := sl.List()
	if len(list) != len(words) {
		t.Fatalf("expected %d entries, got %d", len(words), len(list))
	}

	for i := 1; i < len(list); i++ {
		if list[i].AddedAt.Before(list[i-1].AddedAt) {
			t.Errorf("list not sorted by AddedAt at index %d", i)
		}
	}
}

func TestStopList_Persistence(t *testing.T) {
	f, err := os.CreateTemp("", "stoplist-persist-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	sl1, err := stoplist.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	sl1.Add("кеды")
	sl1.Add("сланцы")
	sl1.Close()

	sl2, err := stoplist.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer sl2.Close()

	if !sl2.Contains("кеды") {
		t.Error("expected 'кеды' to persist across restart")
	}
	if !sl2.Contains("сланцы") {
		t.Error("expected 'сланцы' to persist across restart")
	}
	if len(sl2.List()) != 2 {
		t.Errorf("expected 2 entries after restart, got %d", len(sl2.List()))
	}
}

func TestStopList_Persistence_AfterRemove(t *testing.T) {
	f, err := os.CreateTemp("", "stoplist-rm-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	sl1, err := stoplist.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	sl1.Add("временное")
	sl1.Remove("временное")
	sl1.Close()

	sl2, err := stoplist.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer sl2.Close()

	if sl2.Contains("временное") {
		t.Error("expected removed word to not persist after restart")
	}
}

func TestStopList_ConcurrentReads(t *testing.T) {
	sl, cleanup := newTempStopList(t)
	defer cleanup()

	sl.Add("слово")

	done := make(chan struct{}, 100)
	for range 100 {
		go func() {
			_ = sl.Contains("слово")
			_ = sl.List()
			done <- struct{}{}
		}()
	}
	for range 100 {
		<-done
	}
}
