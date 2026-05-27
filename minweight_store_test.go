package minweight_store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

func TestPutGetDelete(t *testing.T) {
	store := New()

	key := []byte("alpha")
	value := []byte("one")
	if err := store.Put(key, value); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	value[0] = 'z'

	got, ok, err := store.Get([]byte("alpha"))
	if err != nil || !ok || string(got) != "one" {
		t.Fatalf("Get(alpha) = (%q,%v,%v), want (one,true,nil)", got, ok, err)
	}
	got[0] = 'x'
	got, ok, err = store.Get([]byte("alpha"))
	if err != nil || !ok || string(got) != "one" {
		t.Fatalf("Get(alpha) after caller mutation = (%q,%v,%v), want (one,true,nil)", got, ok, err)
	}

	if err := store.Put([]byte("alpha"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	got, ok, err = store.Get([]byte("alpha"))
	if err != nil || !ok || string(got) != "two" {
		t.Fatalf("Get(alpha) after replace = (%q,%v,%v), want (two,true,nil)", got, ok, err)
	}
	if store.Len() != 1 {
		t.Fatalf("Len = %d, want 1", store.Len())
	}

	deleted, err := store.Delete([]byte("alpha"))
	if err != nil || !deleted {
		t.Fatalf("Delete(alpha) = (%v,%v), want (true,nil)", deleted, err)
	}
	deleted, err = store.Delete([]byte("alpha"))
	if err != nil || deleted {
		t.Fatalf("Delete(alpha) again = (%v,%v), want (false,nil)", deleted, err)
	}
	_, ok, err = store.Get([]byte("alpha"))
	if err != nil || ok {
		t.Fatalf("Get(alpha) after delete ok=%v err=%v, want false,nil", ok, err)
	}
}

func TestStoreFatalAfterRecordAcceptedIndexFailure(t *testing.T) {
	keyTooLarge := make([]byte, minpatricia.MaxKeySize+1)
	records := &badKeyRecordStore{
		heapRecordStore: newHeapRecordStore(),
		key:             keyTooLarge,
	}
	store := &Store{
		backend: newIndexBackendWithNodes(records, newHeapNodeStore()),
	}

	err := store.Put([]byte("alpha"), []byte("one"))
	assertFatalError(t, err)
	if !errors.Is(err, minpatricia.ErrKeyTooLarge) {
		t.Fatalf("Put err = %v, want %v", err, minpatricia.ErrKeyTooLarge)
	}
	if records.appends != 1 {
		t.Fatalf("record appends = %d, want 1", records.appends)
	}

	if err := store.Put([]byte("bravo"), []byte("two")); !errors.Is(err, ErrFatal) {
		t.Fatalf("Put after fatal err = %v, want %v", err, ErrFatal)
	}
	if _, _, err := store.Get([]byte("alpha")); !errors.Is(err, ErrFatal) {
		t.Fatalf("Get after fatal err = %v, want %v", err, ErrFatal)
	}
	if _, err := store.Delete([]byte("alpha")); !errors.Is(err, ErrFatal) {
		t.Fatalf("Delete after fatal err = %v, want %v", err, ErrFatal)
	}
	err = store.Scan(func(Item) bool {
		t.Fatal("Scan callback should not run after fatal")
		return true
	})
	if !errors.Is(err, ErrFatal) {
		t.Fatalf("Scan after fatal err = %v, want %v", err, ErrFatal)
	}
	if store.Len() != 0 {
		t.Fatalf("Len after fatal = %d, want 0", store.Len())
	}
}

func TestStoreDoesNotFatalOnRecordAppendFailure(t *testing.T) {
	records := &appendErrorRecordStore{
		heapRecordStore: newHeapRecordStore(),
		err:             ErrWalFull,
	}
	store := &Store{
		backend: newIndexBackendWithNodes(records, newHeapNodeStore()),
	}

	err := store.Put([]byte("alpha"), []byte("one"))
	if !errors.Is(err, ErrWalFull) {
		t.Fatalf("Put err = %v, want %v", err, ErrWalFull)
	}
	if errors.Is(err, ErrFatal) {
		t.Fatalf("Put err = %v, must not be fatal", err)
	}

	_, ok, err := store.Get([]byte("alpha"))
	if err != nil || ok {
		t.Fatalf("Get after append failure ok=%v err=%v, want false,nil", ok, err)
	}
}

func TestScanAndSeek(t *testing.T) {
	store := New()
	for _, key := range []string{"delta", "alpha", "charlie", "bravo", "echo"} {
		if err := store.Put([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Delete([]byte("charlie")); err != nil {
		t.Fatal(err)
	}

	assertItems(t, "Scan", store.Scan, []string{
		"alpha=value-alpha",
		"bravo=value-bravo",
		"delta=value-delta",
		"echo=value-echo",
	})
	assertItems(t, "ReverseScan", store.ReverseScan, []string{
		"echo=value-echo",
		"delta=value-delta",
		"bravo=value-bravo",
		"alpha=value-alpha",
	})

	item, ok, err := store.SeekGE([]byte("caper"))
	if err != nil || !ok || string(item.Key) != "delta" || string(item.Value) != "value-delta" {
		t.Fatalf("SeekGE(caper) = (%q,%q,%v,%v), want delta,value-delta,true,nil", item.Key, item.Value, ok, err)
	}
	item, ok, err = store.SeekLE([]byte("caper"))
	if err != nil || !ok || string(item.Key) != "bravo" || string(item.Value) != "value-bravo" {
		t.Fatalf("SeekLE(caper) = (%q,%q,%v,%v), want bravo,value-bravo,true,nil", item.Key, item.Value, ok, err)
	}
	_, ok, err = store.SeekGE([]byte("zulu"))
	if err != nil || ok {
		t.Fatalf("SeekGE(zulu) ok=%v err=%v, want false,nil", ok, err)
	}
	_, ok, err = store.SeekLE([]byte("aardvark"))
	if err != nil || ok {
		t.Fatalf("SeekLE(aardvark) ok=%v err=%v, want false,nil", ok, err)
	}
}

func TestScanRange(t *testing.T) {
	store := New()
	for _, key := range []string{"a", "aa", "b", "ba", "c"} {
		if err := store.Put([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	assertItems(t, "ScanRange", func(fn VisitFunc) error {
		return store.ScanRange([]byte("aa"), []byte("c"), fn)
	}, []string{
		"aa=aa",
		"b=b",
		"ba=ba",
	})

	assertItems(t, "ScanRange/unbound-upper", func(fn VisitFunc) error {
		return store.ScanRange([]byte("b"), nil, fn)
	}, []string{
		"b=b",
		"ba=ba",
		"c=c",
	})

	assertItems(t, "ReverseScanRange", func(fn VisitFunc) error {
		return store.ReverseScanRange([]byte("ba"), []byte("a"), fn)
	}, []string{
		"ba=ba",
		"b=b",
		"aa=aa",
	})

	assertItems(t, "ReverseScanRange/unbound-lower", func(fn VisitFunc) error {
		return store.ReverseScanRange([]byte("ba"), nil, fn)
	}, []string{
		"ba=ba",
		"b=b",
		"aa=aa",
		"a=a",
	})

	err := store.ScanRange([]byte("z"), []byte("a"), func(Item) bool {
		t.Fatal("invalid range callback should not run")
		return true
	})
	if !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("ScanRange invalid err = %v, want %v", err, ErrInvalidRange)
	}
}

func TestScanEarlyStop(t *testing.T) {
	store := New()
	for _, key := range []string{"alpha", "bravo", "charlie"} {
		if err := store.Put([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	err := store.Scan(func(item Item) bool {
		got = append(got, string(item.Key))
		return len(got) < 2
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[alpha bravo]" {
		t.Fatalf("early stop = %v, want [alpha bravo]", got)
	}
}

func assertItems(t *testing.T, name string, scan func(VisitFunc) error, want []string) {
	t.Helper()

	var got []string
	err := scan(func(item Item) bool {
		got = append(got, fmt.Sprintf("%s=%s", item.Key, item.Value))
		return true
	})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertFatalError(t *testing.T, err error) {
	t.Helper()

	if !errors.Is(err, ErrFatal) {
		t.Fatalf("err = %v, want %v", err, ErrFatal)
	}
}

type badKeyRecordStore struct {
	*heapRecordStore
	key     []byte
	appends int
}

func (s *badKeyRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	s.appends++
	return s.heapRecordStore.Append(key, value)
}

func (s *badKeyRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	return s.key, true
}

type appendErrorRecordStore struct {
	*heapRecordStore
	err error
}

func (s *appendErrorRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	return 0, s.err
}
