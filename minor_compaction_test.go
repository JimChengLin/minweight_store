//go:build darwin || linux

package minweight_store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMinorCompactRetargetsCheckpointedWALToParquet(t *testing.T) {
	store := openMinorCompactionStoreForTest(t)
	defer closeForTest(t, store)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	assertGet(t, store, "charlie", "three")

	assertIndexFileNoForKey(t, store, "alpha", onlyParquetFileNoForTest(t, store))
	assertIndexFileNoForKey(t, store, "bravo", onlyParquetFileNoForTest(t, store))
}

func TestMinorCompactWritesParquetUnderSSTDir(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	defer closeForTest(t, store)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	parquetName := parquetSegmentName(onlyParquetFileNoForTest(t, store))
	if _, err := os.Stat(filepath.Join(sstSegmentsPath(dir), parquetName)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(walSegmentsPath(dir), parquetName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("parquet under WAL dir err = %v, want not exist", err)
	}

	assertDirEntrySuffixes(t, walSegmentsPath(dir), walSegmentSuffix)
	assertDirEntrySuffixes(t, sstSegmentsPath(dir), parquetSegmentSuffix)
}

func TestMinorCompactMemoryStoreNoop(t *testing.T) {
	store := New()

	if err := store.minorCompact(); err != nil {
		t.Fatalf("minorCompact memory store err = %v, want nil", err)
	}
}

func TestMinorCompactDeleteOnlyWALPublishesEmptyParquet(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:            crashTestWALSize,
		MaxImmutableWALNum: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Delete([]byte("alpha"))
	if err != nil || !deleted {
		t.Fatalf("Delete(alpha) = (%v,%v), want true,nil", deleted, err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	compacted, err := store.minorCompactWAL(firstWALSegmentNo + 1)
	if err != nil || !compacted {
		t.Fatalf("minorCompactWAL(delete-only) = (%v,%v), want true,nil", compacted, err)
	}
	parquetFileNo := onlyParquetFileNoForTest(t, store)
	parquetStore := parquetSegmentForTest(t, store, parquetFileNo)
	if parquetStore.Len() != 0 {
		t.Fatalf("empty parquet len = %d, want 0", parquetStore.Len())
	}
	if _, ok, err := store.Get([]byte("alpha")); err != nil || ok {
		t.Fatalf("Get(alpha) after delete-only compact ok=%v err=%v, want false,nil", ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, reopened)
	if _, ok, err := reopened.Get([]byte("alpha")); err != nil || ok {
		t.Fatalf("Get(alpha) after reopen ok=%v err=%v, want false,nil", ok, err)
	}
	reopenedParquetFileNo := onlyParquetFileNoForTest(t, reopened)
	reopenedParquetStore := parquetSegmentForTest(t, reopened, reopenedParquetFileNo)
	if reopenedParquetStore.Len() != 0 {
		t.Fatalf("reopened empty parquet len = %d, want 0", reopenedParquetStore.Len())
	}
}

func TestMinorCompactInstallSSTReplaysAfterDirtyRestart(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, reopened)

	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactSurvivesGracefulClose(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, reopened)

	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactDoesNotTakeSecondaryIndexMu(t *testing.T) {
	store := openMinorCompactionStoreForTest(t)
	defer closeForTest(t, store)

	store.secondaryIndexMu.Lock()
	secondaryLocked := true
	defer func() {
		if secondaryLocked {
			store.secondaryIndexMu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- store.minorCompact()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("minorCompact blocked on secondaryIndexMu")
	}
	store.secondaryIndexMu.Unlock()
	secondaryLocked = false
}

func TestCloseWaitsForCompaction(t *testing.T) {
	store := openMinorCompactionStoreForTest(t)

	store.compactionMu.RLock()
	compactionLocked := true
	defer func() {
		if compactionLocked {
			store.compactionMu.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- store.Close()
	}()

	select {
	case err := <-done:
		store.compactionMu.RUnlock()
		compactionLocked = false
		t.Fatalf("Close returned before compaction finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	store.compactionMu.RUnlock()
	compactionLocked = false
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func openMinorCompactionStoreForTest(t *testing.T) *Store {
	t.Helper()

	return openMinorCompactionStoreInDirForTest(t, t.TempDir())
}

func openMinorCompactionStoreInDirForTest(t *testing.T, dir string) *Store {
	t.Helper()

	store, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("charlie"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	return store
}

func onlyParquetFileNoForTest(t *testing.T, store *Store) uint64 {
	t.Helper()

	var fileNo uint64
	count := 0
	store.records.mu.RLock()
	defer store.records.mu.RUnlock()
	for id, segment := range store.records.segments {
		if _, ok := segment.(*parquetRecordStore); ok {
			fileNo = id
			count++
		}
	}
	if count != 1 {
		t.Fatalf("parquet segment count = %d, want 1", count)
	}
	return fileNo
}

func parquetSegmentForTest(t *testing.T, store *Store, fileNo uint64) *parquetRecordStore {
	t.Helper()

	store.records.mu.RLock()
	defer store.records.mu.RUnlock()
	segment, ok := store.records.segments[fileNo].(*parquetRecordStore)
	if !ok {
		t.Fatalf("segment %d is not parquet", fileNo)
	}
	return segment
}

func assertIndexFileNoForKey(t *testing.T, store *Store, key string, want uint64) {
	t.Helper()

	store.primaryMu.RLock()
	defer store.primaryMu.RUnlock()
	pos, ok, err := store.backend.index.Get([]byte(key))
	if err != nil || !ok {
		t.Fatalf("index.Get(%s) = (%d,%v,%v), want position,true,nil", key, pos, ok, err)
	}
	if got := recordPositionFileNo(pos); got != want {
		t.Fatalf("index fileNo for %s = %d, want %d", key, got, want)
	}
}

func assertDirEntrySuffixes(t *testing.T, dir, suffix string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), suffix) {
			t.Fatalf("entry %s in %s, want only %s files", entry.Name(), dir, suffix)
		}
	}
}
