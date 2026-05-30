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

func TestMinorCompactDeletesSourceWALOnNextFlush(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	defer closeForTest(t, store)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	if !walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL deleted before install_sst checkpoint")
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL still exists after install_sst checkpoint")
	}

	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	assertGet(t, store, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, store)
	assertIndexFileNoForKey(t, store, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, store, "bravo", parquetFileNo)
}

func TestMinorCompactDeletesSourceWALAfterDirtyInstallSSTReplay(t *testing.T) {
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
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)

	if walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL still exists after dirty install_sst replay checkpoint")
	}
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactDeletedSourceWALBeforeFinalManifestRecovers(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	simulateCheckpointAfterPendingWALDeleteBeforeManifestForTest(t, store)
	if walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL still exists before simulated crash")
	}

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)

	if walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL was recreated during primary_wal_flushed recovery")
	}
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactFinalManifestDoesNotNeedDeletedSourceWAL(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL still exists after final manifest")
	}
	dirtySyncAndCloseStoreForTest(t, store)

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)

	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactPrimaryFlushedRecoveryDeletesSourceWAL(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	simulatePrimaryWALFlushedCheckpointForTest(t, store)

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)

	if walFileExistsForTest(t, dir, firstWALSegmentNo) {
		t.Fatal("source WAL still exists after primary_wal_flushed recovery")
	}
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactMemoryStoreNoop(t *testing.T) {
	store := New()

	if err := store.minorCompact(); err != nil {
		t.Fatalf("minorCompact memory store err = %v, want nil", err)
	}
}

func TestMinorCompactCompactsMultipleCandidates(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MinorCompactionThreadNum: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.stopMinorCompactionDispatcher()
	defer closeForTest(t, store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
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
	if err := store.Put([]byte("delta"), []byte("four")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	waitForIndexFileNoChangeForTest(t, store, "alpha", firstWALSegmentNo)
	waitForIndexFileNoChangeForTest(t, store, "charlie", firstWALSegmentNo+1)
	assertIndexFileNoForKey(t, store, "delta", firstWALSegmentNo+2)
	if got := len(parquetFileNosForTest(store)); got != 2 {
		t.Fatalf("parquet segment count = %d, want 2", got)
	}
}

func TestMinorCompactMultipleWALFlushReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize: crashTestWALSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.stopMinorCompactionDispatcher()

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("delta"), []byte("four")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}
	for fileNo := uint64(firstWALSegmentNo); fileNo <= firstWALSegmentNo+2; fileNo++ {
		if !walFileExistsForTest(t, dir, fileNo) {
			t.Fatalf("source WAL %d deleted before install_sst checkpoint", fileNo)
		}
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	for fileNo := uint64(firstWALSegmentNo); fileNo <= firstWALSegmentNo+2; fileNo++ {
		if walFileExistsForTest(t, dir, fileNo) {
			t.Fatalf("source WAL %d still exists after install_sst checkpoint", fileNo)
		}
	}
	assertGet(t, store, "alpha", "three")
	assertGet(t, store, "bravo", "two")
	assertGet(t, store, "delta", "four")
	assertIndexNotFileNoForKey(t, store, "alpha", firstWALSegmentNo+2)
	assertIndexNotFileNoForKey(t, store, "bravo", firstWALSegmentNo+1)
	assertIndexFileNoForKey(t, store, "delta", firstWALSegmentNo+3)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)

	assertGet(t, reopened, "alpha", "three")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "delta", "four")
	assertIndexNotFileNoForKey(t, reopened, "alpha", firstWALSegmentNo+2)
	assertIndexNotFileNoForKey(t, reopened, "bravo", firstWALSegmentNo+1)
	assertIndexFileNoForKey(t, reopened, "delta", firstWALSegmentNo+3)
	for fileNo := uint64(firstWALSegmentNo); fileNo <= firstWALSegmentNo+2; fileNo++ {
		if walFileExistsForTest(t, dir, fileNo) {
			t.Fatalf("source WAL %d reappeared after reopen", fileNo)
		}
	}
}

func TestMinorCompactionWorkersCompactCheckpointedWAL(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MinorCompactionThreadNum: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

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

	waitForIndexFileNoChangeForTest(t, store, "alpha", firstWALSegmentNo)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	assertGet(t, store, "charlie", "three")
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
	store.stopMinorCompactionDispatcher()
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
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	if walFileExistsForTest(t, dir, firstWALSegmentNo+1) {
		t.Fatal("delete-only source WAL still exists after install_sst checkpoint")
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

func TestMinorCompactParquetTmpCrashIsCleanedOnOpen(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	dirtySyncAndCloseStoreForTest(t, store)

	tmpFileNo := firstUnallocatedFileNoForTest(t, dir)
	tmpPath := parquetSegmentPath(dir, tmpFileNo) + ".tmp"
	if err := os.WriteFile(tmpPath, []byte("partial parquet"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, Options{
		WALSize: crashTestWALSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	if fileExistsForTest(t, tmpPath) {
		t.Fatal("tmp parquet still exists after Open")
	}

	if err := reopened.minorCompact(); err != nil {
		t.Fatal(err)
	}
	parquetFileNo := onlyParquetFileNoForTest(t, reopened)
	if parquetFileNo != tmpFileNo {
		t.Fatalf("minor compact parquet fileNo = %d, want reused tmp fileNo %d", parquetFileNo, tmpFileNo)
	}
	assertIndexFileNoForKey(t, reopened, "alpha", parquetFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", parquetFileNo)
}

func TestMinorCompactParquetWithoutInstallSSTIsCleanedAndReused(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	orphanFileNo, _ := buildParquetFromWALForTest(t, store, firstWALSegmentNo)
	dirtySyncAndCloseStoreForTest(t, store)

	reopened, err := Open(dir, Options{
		WALSize: crashTestWALSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)
	if got := parquetFileNosForTest(reopened); len(got) != 0 {
		t.Fatalf("opened parquet segments = %v, want none before install_sst", got)
	}
	if fileExistsForTest(t, parquetSegmentPath(dir, orphanFileNo)) {
		t.Fatal("uninstalled parquet still exists after Open")
	}
	assertIndexFileNoForKey(t, reopened, "alpha", firstWALSegmentNo)

	if err := reopened.minorCompact(); err != nil {
		t.Fatal(err)
	}
	if got := onlyParquetFileNoForTest(t, reopened); got != orphanFileNo {
		t.Fatalf("minor compact parquet fileNo = %d, want reused orphan %d", got, orphanFileNo)
	}
	assertIndexFileNoForKey(t, reopened, "alpha", orphanFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", orphanFileNo)
}

func TestMinorCompactUninstalledParquetCleanupAllowsWALRollover(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	orphanFileNo, _ := buildParquetFromWALForTest(t, store, firstWALSegmentNo)
	dirtySyncAndCloseStoreForTest(t, store)

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	if fileExistsForTest(t, parquetSegmentPath(dir, orphanFileNo)) {
		t.Fatal("uninstalled parquet still exists after Open")
	}
	if err := reopened.Put([]byte("echo"), []byte("five")); err != nil {
		t.Fatal(err)
	}
	if err := reopened.flush(); err != nil {
		t.Fatal(err)
	}
	if !walFileExistsForTest(t, dir, orphanFileNo) {
		t.Fatalf("WAL %d was not created after orphan parquet cleanup", orphanFileNo)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err = Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)
	assertGet(t, reopened, "echo", "five")
}

func TestMinorCompactWALAndSSTSameFileNoFailsFast(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	parquetStore, err := createParquetRecordStore(parquetSegmentPath(dir, firstWALSegmentNo), firstWALSegmentNo)
	if err != nil {
		t.Fatal(err)
	}
	if err := parquetStore.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := parquetStore.Close(); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	_, err = Open(dir, Options{WALSize: crashTestWALSize})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("Open with WAL/SST fileNo collision err = %v, want %v", err, ErrManifest)
	}
}

func TestMinorCompactInstallSSTWALReplaysBeforeRuntimeRetarget(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	sstFileNo, _ := buildParquetFromWALForTest(t, store, firstWALSegmentNo)
	if _, err := store.records.AppendInstallSSTRecord(firstWALSegmentNo, sstFileNo); err != nil {
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
	assertIndexFileNoForKey(t, reopened, "alpha", sstFileNo)
	assertIndexFileNoForKey(t, reopened, "bravo", sstFileNo)
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

func TestMinorCompactCorruptInstallSSTReplayPolicy(t *testing.T) {
	t.Run("strict_fails", func(t *testing.T) {
		dir := compactWithCorruptInstallSSTForTest(t)
		_, err := Open(dir, Options{
			WALSize:         crashTestWALSize,
			WALReplayPolicy: WALReplayStrict,
		})
		if !errors.Is(err, ErrCorruptWAL) {
			t.Fatalf("Open strict err = %v, want %v", err, ErrCorruptWAL)
		}
	})

	t.Run("point_in_time_keeps_old_wal_index", func(t *testing.T) {
		dir := compactWithCorruptInstallSSTForTest(t)
		store, err := Open(dir, Options{
			WALSize:         crashTestWALSize,
			WALReplayPolicy: WALReplayPointInTime,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeForTest(t, store)
		assertGet(t, store, "alpha", "one")
		assertGet(t, store, "bravo", "two")
		assertIndexFileNoForKey(t, store, "alpha", firstWALSegmentNo)
		assertIndexFileNoForKey(t, store, "bravo", firstWALSegmentNo)
	})

	t.Run("best_effort_repairs_and_keeps_old_wal_index", func(t *testing.T) {
		dir := compactWithCorruptInstallSSTForTest(t)
		store, err := Open(dir, Options{
			WALSize:         crashTestWALSize,
			WALReplayPolicy: WALReplayBestEffort,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeForTest(t, store)
		assertGet(t, store, "alpha", "one")
		assertGet(t, store, "bravo", "two")
		assertIndexFileNoForKey(t, store, "alpha", firstWALSegmentNo)
		assertIndexFileNoForKey(t, store, "bravo", firstWALSegmentNo)
	})
}

func TestMinorCompactPointInTimeCleanupPreventsParquetWALFileNoCollision(t *testing.T) {
	dir := compactWithCorruptInstallSSTForTest(t)
	orphanFileNo := firstUnallocatedFileNoForTest(t, dir)

	store, err := Open(dir, Options{
		WALSize:         crashTestWALSize,
		WALReplayPolicy: WALReplayPointInTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.stopMinorCompactionDispatcher()
	if fileExistsForTest(t, parquetSegmentPath(dir, orphanFileNo)) {
		t.Fatalf("orphan parquet %d still exists after point-in-time recovery", orphanFileNo)
	}
	if !walFileExistsForTest(t, dir, orphanFileNo) {
		t.Fatalf("WAL %d was not created after orphan parquet cleanup", orphanFileNo)
	}
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	assertIndexFileNoForKey(t, store, "alpha", firstWALSegmentNo)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
}

func TestMinorCompactInstallSSTMissingParquetFailsFast(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	sstFileNo, _ := buildParquetFromWALForTest(t, store, firstWALSegmentNo)
	if _, err := store.records.AppendInstallSSTRecord(firstWALSegmentNo, sstFileNo); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(parquetSegmentPath(dir, sstFileNo)); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	_, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err == nil {
		t.Fatal("Open with missing install_sst parquet err = nil, want error")
	}
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
	store.stopMinorCompactionDispatcher()
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

func parquetFileNosForTest(store *Store) []uint64 {
	store.records.mu.RLock()
	defer store.records.mu.RUnlock()

	var fileNos []uint64
	for id, segment := range store.records.segments {
		if _, ok := segment.(*parquetRecordStore); ok {
			fileNos = append(fileNos, id)
		}
	}
	return fileNos
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

func assertIndexNotFileNoForKey(t *testing.T, store *Store, key string, old uint64) {
	t.Helper()

	store.primaryMu.RLock()
	defer store.primaryMu.RUnlock()
	pos, ok, err := store.backend.index.Get([]byte(key))
	if err != nil || !ok {
		t.Fatalf("index.Get(%s) = (%d,%v,%v), want position,true,nil", key, pos, ok, err)
	}
	if got := recordPositionFileNo(pos); got == old {
		t.Fatalf("index fileNo for %s = %d, want different fileNo", key, got)
	}
}

func waitForIndexFileNoChangeForTest(t *testing.T, store *Store, key string, old uint64) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.primaryMu.RLock()
		pos, ok, err := store.backend.index.Get([]byte(key))
		store.primaryMu.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
		if ok && recordPositionFileNo(pos) != old {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("index fileNo for %s did not change from %d", key, old)
}

func firstUnallocatedFileNoForTest(t *testing.T, dir string) uint64 {
	t.Helper()

	manifest, state, ok, err := openManifest(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manifest.close(); err != nil {
			t.Fatal(err)
		}
	}()
	if !ok {
		t.Fatal("manifest missing")
	}
	return state.nextFileNo
}

func buildParquetFromWALForTest(t *testing.T, store *Store, sourceWALFileNo uint64) (uint64, []walCompactionCandidate) {
	t.Helper()

	candidates, hasKVRecord, err := sortedWALPutCompactionCandidates(store.records, sourceWALFileNo)
	if err != nil {
		t.Fatal(err)
	}
	if !hasKVRecord {
		t.Fatal("source WAL has no KV records")
	}

	var parquetStore *parquetRecordStore
	liveCandidates := candidates[:0]
	for _, candidate := range candidates {
		current, ok, err := store.currentIndexPosition(candidate.key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || current != candidate.pos {
			continue
		}
		if parquetStore == nil {
			parquetStore, err = store.records.createParquetSegment()
			if err != nil {
				t.Fatal(err)
			}
		}
		value, ok := store.records.Value(candidate.pos)
		if !ok {
			t.Fatal(ErrCorruptWAL)
		}
		if _, err := parquetStore.Append(candidate.key, value); err != nil {
			t.Fatal(err)
		}
		liveCandidates = append(liveCandidates, candidate)
	}
	if parquetStore == nil {
		var err error
		parquetStore, err = store.records.createParquetSegment()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := parquetStore.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.records.installParquetSegment(parquetStore); err != nil {
		t.Fatal(err)
	}
	return parquetStore.fileNo, liveCandidates
}

func compactWithCorruptInstallSSTForTest(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	sstFileNo, _ := buildParquetFromWALForTest(t, store, firstWALSegmentNo)
	pos, err := store.records.AppendInstallSSTRecord(firstWALSegmentNo, sstFileNo)
	if err != nil {
		t.Fatal(err)
	}
	active := store.records.activeSegment()
	active.data[recordPositionOffset(pos)+walRecordHeaderSize] ^= 0xff
	dirtySyncAndCloseStoreForTest(t, store)
	return dir
}

func simulateCheckpointAfterPendingWALDeleteBeforeManifestForTest(t *testing.T, store *Store) {
	t.Helper()

	store.stopMinorCompactionDispatcher()
	oldWALFileNo := store.records.activeFileNo
	oldWAL, err := store.records.Rollover()
	if err != nil {
		t.Fatal(err)
	}
	active := store.records.activeSegment()
	if err := syncPrimaryIndexAndWAL(store.backend, oldWAL); err != nil {
		t.Fatal(err)
	}
	if err := active.Sync(); err != nil {
		t.Fatal(err)
	}
	state := manifestState{
		checkpointWALFileNo: oldWALFileNo,
		activeWALFileNo:     store.records.activeFileNo,
		nextFileNo:          store.records.nextFileNo,
		walSegmentSize:      uint64(store.records.size),
		primaryWALFlushed:   true,
	}
	if err := store.manifest.write(state); err != nil {
		t.Fatal(err)
	}
	if err := checkpointSecondaryIndex(store.manifest.dir(), store.records, oldWALFileNo, WALReplayStrict, store.backend.index); err != nil {
		t.Fatal(err)
	}
	if err := store.records.deletePendingWALs(); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)
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

func walFileExistsForTest(t *testing.T, dir string, fileNo uint64) bool {
	t.Helper()

	return fileExistsForTest(t, filepath.Join(walSegmentsPath(dir), walSegmentName(fileNo)))
}

func fileExistsForTest(t *testing.T, path string) bool {
	t.Helper()

	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	t.Fatal(err)
	return false
}
