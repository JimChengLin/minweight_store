//go:build darwin || linux

package minweight_store

import "testing"

func TestMajorCompactRewritesAllParquetSegments(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	defer closeForTest(t, store)

	oldSSTs := parquetFileNosForTest(store)
	if len(oldSSTs) != 2 {
		t.Fatalf("old parquet count = %d, want 2", len(oldSSTs))
	}

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	newSSTs := parquetFileNosForTest(store)
	if len(newSSTs) != 4 {
		t.Fatalf("new parquet count = %d, want 4", len(newSSTs))
	}
	assertNoFileNoOverlapForTest(t, oldSSTs, newSSTs)
	for _, fileNo := range oldSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after checkpoint", fileNo)
		}
	}
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	assertGet(t, store, "charlie", "three")
	assertGet(t, store, "delta", "four")
}

func TestMajorCompactLimitsInputSSTCount(t *testing.T) {
	dir := t.TempDir()
	workers := 2
	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MajorCompactionThreadNum: workers,
		TargetSSTSize:            1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	store.stopMinorCompactionDispatcher()

	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"}
	for _, key := range keys {
		if err := store.Put([]byte(key), []byte(key+"-value")); err != nil {
			t.Fatal(err)
		}
		if err := store.flush(); err != nil {
			t.Fatal(err)
		}
		sourceWAL := store.checkpointWALFileNo
		compacted, err := store.minorCompactWAL(sourceWAL)
		if err != nil || !compacted {
			t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", sourceWAL, compacted, err)
		}
		if err := store.flush(); err != nil {
			t.Fatal(err)
		}
	}

	wantSelected := workers * majorCompactionMaxSSTsPerWorker
	allSSTs := store.records.compactableParquetFileNos()
	if len(allSSTs) != wantSelected+1 {
		t.Fatalf("compactable parquet count = %d, want %d", len(allSSTs), wantSelected+1)
	}
	selectedSSTs, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != wantSelected {
		t.Fatalf("major compaction candidate count = %d, want %d", len(selectedSSTs), wantSelected)
	}
	unselectedSST := allSSTs[wantSelected]

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	for _, fileNo := range selectedSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("selected parquet %d still exists after checkpoint", fileNo)
		}
	}
	if !fileExistsForTest(t, parquetSegmentPath(dir, unselectedSST)) {
		t.Fatalf("unselected parquet %d was compacted", unselectedSST)
	}
	for _, key := range keys {
		assertGet(t, store, key, key+"-value")
	}
}

func TestMajorCompactInstallSSTBatchReplaysAfterDirtyRestart(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	oldSSTs := parquetFileNosForTest(store)

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	reopened, err := Open(dir, Options{
		WALSize:       crashTestWALSize,
		TargetSSTSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, reopened)

	newSSTs := parquetFileNosForTest(reopened)
	if len(newSSTs) != 4 {
		t.Fatalf("reopened parquet count = %d, want 4", len(newSSTs))
	}
	assertNoFileNoOverlapForTest(t, oldSSTs, newSSTs)
	for _, fileNo := range oldSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after recovery", fileNo)
		}
	}
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "bravo", "two")
	assertGet(t, reopened, "charlie", "three")
	assertGet(t, reopened, "delta", "four")
}

func TestMajorCompactBuiltParquetWithoutInstallSSTBatchIsCleaned(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	oldSSTs := parquetFileNosForTest(store)
	newSSTs, _, err := store.buildMajorCompactionSSTs(oldSSTs)
	if err != nil {
		t.Fatal(err)
	}
	newSSTFileNos := parquetStoreFileNos(newSSTs)
	for _, sst := range newSSTs {
		if err := sst.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Put([]byte("echo"), []byte("five")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	reopened, err := Open(dir, Options{
		WALSize:       crashTestWALSize,
		TargetSSTSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	reopened.stopMinorCompactionDispatcher()
	defer closeForTest(t, reopened)

	gotSSTs := recordFileNoSet(parquetFileNosForTest(reopened))
	for _, fileNo := range oldSSTs {
		if _, ok := gotSSTs[fileNo]; !ok {
			t.Fatalf("old parquet %d is not live after Open", fileNo)
		}
	}
	for _, fileNo := range newSSTFileNos {
		if _, ok := gotSSTs[fileNo]; ok {
			t.Fatalf("uninstalled major parquet %d opened as live", fileNo)
		}
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("uninstalled major parquet %d still exists after Open", fileNo)
		}
	}
	assertGet(t, reopened, "alpha", "one")
	assertGet(t, reopened, "echo", "five")
}

func TestMajorCompactMergeSortsSSTStreams(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	store.stopMinorCompactionDispatcher()

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("zulu"), []byte("last")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	firstSource := store.checkpointWALFileNo
	if compacted, err := store.minorCompactWAL(firstSource); err != nil || !compacted {
		t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", firstSource, compacted, err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	if err := store.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("gamma"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	secondSource := store.checkpointWALFileNo
	if compacted, err := store.minorCompactWAL(secondSource); err != nil || !compacted {
		t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", secondSource, compacted, err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	sst := parquetSegmentForTest(t, store, onlyParquetFileNoForTest(t, store))
	var keys []string
	if err := sst.scanKeys(func(rowIndex uint64, key []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha", "beta", "gamma", "zulu"}
	if len(keys) != len(want) {
		t.Fatalf("major compact keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("major compact keys = %v, want %v", keys, want)
		}
	}
}

func openMajorCompactionStoreForTest(t *testing.T, dir string) *Store {
	t.Helper()

	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MajorCompactionThreadNum: 2,
		TargetSSTSize:            1,
	})
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
	if err := store.Put([]byte("delta"), []byte("four")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	for _, fileNo := range []uint64{firstWALSegmentNo, firstWALSegmentNo + 1} {
		compacted, err := store.minorCompactWAL(fileNo)
		if err != nil || !compacted {
			t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", fileNo, compacted, err)
		}
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	return store
}

func assertNoFileNoOverlapForTest(t *testing.T, oldFileNos, newFileNos []uint64) {
	t.Helper()

	oldSet := recordFileNoSet(oldFileNos)
	for _, fileNo := range newFileNos {
		if _, ok := oldSet[fileNo]; ok {
			t.Fatalf("fileNo %d appears in both old and new sets", fileNo)
		}
	}
}
