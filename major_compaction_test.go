//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/JimChengLin/minpatricia"
)

func TestMajorCompactPicksGarbageParquetSegments(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	defer closeForTest(t, store)

	oldSSTs := parquetFileNosForTest(store)
	if len(oldSSTs) != 3 {
		t.Fatalf("old parquet count = %d, want 3", len(oldSSTs))
	}
	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Delete([]byte("charlie"))
	if err != nil || !deleted {
		t.Fatalf("Delete(charlie) = (%v,%v), want true,nil", deleted, err)
	}
	if err := store.Put([]byte("echo"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	selectedSSTs, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != len(oldSSTs) {
		t.Fatalf("major compaction candidate count = %d, want %d", len(selectedSSTs), len(oldSSTs))
	}

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	newSSTs := parquetFileNosForTest(store)
	if len(newSSTs) != 3 {
		t.Fatalf("new parquet count = %d, want 3", len(newSSTs))
	}
	assertNoFileNoOverlapForTest(t, oldSSTs, newSSTs)
	for _, fileNo := range oldSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after checkpoint", fileNo)
		}
	}
	liveStats := manifestLiveSSTStatsForTest(t, store.manifest.path)
	for _, fileNo := range oldSSTs {
		if _, ok := liveStats[fileNo]; ok {
			t.Fatalf("old parquet %d still live in manifest after checkpoint", fileNo)
		}
	}
	for _, fileNo := range newSSTs {
		stats, ok := liveStats[fileNo]
		if !ok {
			t.Fatalf("new parquet %d missing from manifest", fileNo)
		}
		if stats.totalEntries != 1 || stats.deletedEntries != 0 {
			t.Fatalf("new parquet %d stats = total %d deleted %d, want 1,0", fileNo, stats.totalEntries, stats.deletedEntries)
		}
	}
	assertGet(t, store, "alpha", "updated")
	assertGet(t, store, "bravo", "two")
	assertMissing(t, store, "charlie")
	assertGet(t, store, "delta", "four")
	assertGet(t, store, "echo", "updated")
	assertGet(t, store, "foxtrot", "six")
}

func TestMajorCompactSkipsBelowGarbageRatio(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	defer closeForTest(t, store)
	store.records.setMaxGarbageRatioPerSST(0.75)

	oldSSTs := parquetFileNosForTest(store)
	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Delete([]byte("charlie"))
	if err != nil || !deleted {
		t.Fatalf("Delete(charlie) = (%v,%v), want true,nil", deleted, err)
	}
	selectedSSTs, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != 0 {
		t.Fatalf("major compaction candidate count = %d, want 0", len(selectedSSTs))
	}

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	gotSSTs := parquetFileNosForTest(store)
	if len(gotSSTs) != len(oldSSTs) {
		t.Fatalf("parquet count after skipped major compaction = %d, want %d", len(gotSSTs), len(oldSSTs))
	}
	for i := range oldSSTs {
		if gotSSTs[i] != oldSSTs[i] {
			t.Fatalf("parquet fileNos after skipped major compaction = %v, want %v", gotSSTs, oldSSTs)
		}
	}
	assertGet(t, store, "alpha", "updated")
	assertMissing(t, store, "charlie")
}

func TestMajorCompactFinalCheckCompactsSingleSSTWhenOverallGarbageHigh(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
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
	oldSST := onlyParquetFileNoForTest(t, store)
	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}

	rawCompactableSSTs := store.records.compactableParquetFileNos()
	if len(rawCompactableSSTs) != 1 || rawCompactableSSTs[0] != oldSST {
		t.Fatalf("raw compactable parquet candidates = %v, want [%d]", rawCompactableSSTs, oldSST)
	}
	selectedSSTs, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != 1 || selectedSSTs[0] != oldSST {
		t.Fatalf("major compaction candidates = %v, want [%d]", selectedSSTs, oldSST)
	}
	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	newSSTs := parquetFileNosForTest(store)
	if len(newSSTs) != 1 {
		t.Fatalf("parquet count after major compaction = %d, want 1", len(newSSTs))
	}
	if newSSTs[0] == oldSST {
		t.Fatalf("single parquet %d was not compacted", oldSST)
	}
	if fileExistsForTest(t, parquetSegmentPath(dir, oldSST)) {
		t.Fatalf("old parquet %d still exists after checkpoint", oldSST)
	}
	assertGet(t, store, "alpha", "updated")
	assertGet(t, store, "bravo", "two")
}

func TestMajorCompactFinalCheckCompactsTwoSSTsWhenOverallGarbageHigh(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
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
	if err := store.Put([]byte("charlie"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("delta"), []byte("four")); err != nil {
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
	oldSSTs := parquetFileNosForTest(store)
	if len(oldSSTs) != 2 {
		t.Fatalf("old parquet count = %d, want 2", len(oldSSTs))
	}

	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Delete([]byte("charlie"))
	if err != nil || !deleted {
		t.Fatalf("Delete(charlie) = (%v,%v), want true,nil", deleted, err)
	}
	if got := len(store.records.compactableParquetFileNos()); got != 2 {
		t.Fatalf("raw compactable parquet count = %d, want 2", got)
	}
	selectedSSTs, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != len(oldSSTs) {
		t.Fatalf("major compaction candidate count = %d, want %d", len(selectedSSTs), len(oldSSTs))
	}
	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	gotSSTs := parquetFileNosForTest(store)
	if len(gotSSTs) == 0 {
		t.Fatalf("parquet count after major compaction = 0, want live SSTs")
	}
	assertNoFileNoOverlapForTest(t, oldSSTs, gotSSTs)
	for _, fileNo := range oldSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after checkpoint", fileNo)
		}
	}
	assertGet(t, store, "alpha", "updated")
	assertGet(t, store, "bravo", "two")
	assertMissing(t, store, "charlie")
	assertGet(t, store, "delta", "four")
}

func TestMajorCompactSkipsSmallTailWhenOverallGarbageLow(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

	for i := 0; i < 5; i++ {
		for j := 0; j < 2; j++ {
			key := fmt.Sprintf("key-%d-%d", i, j)
			if err := store.Put([]byte(key), []byte("value")); err != nil {
				t.Fatal(err)
			}
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
	oldSSTs := parquetFileNosForTest(store)
	if len(oldSSTs) != 5 {
		t.Fatalf("old parquet count = %d, want 5", len(oldSSTs))
	}
	if err := store.Put([]byte("key-0-0"), []byte("updated")); err != nil {
		t.Fatal(err)
	}

	rawCompactableSSTs := store.records.compactableParquetFileNos()
	if len(rawCompactableSSTs) != 1 || rawCompactableSSTs[0] != oldSSTs[0] {
		t.Fatalf("raw compactable parquet candidates = %v, want [%d]", rawCompactableSSTs, oldSSTs[0])
	}
	selectedSSTs, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != 0 {
		t.Fatalf("major compaction candidates = %v, want none", selectedSSTs)
	}
	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}

	gotSSTs := parquetFileNosForTest(store)
	if len(gotSSTs) != len(oldSSTs) {
		t.Fatalf("parquet count after skipped major compaction = %d, want %d", len(gotSSTs), len(oldSSTs))
	}
	for i := range oldSSTs {
		if gotSSTs[i] != oldSSTs[i] {
			t.Fatalf("parquet fileNos after skipped major compaction = %v, want %v", gotSSTs, oldSSTs)
		}
	}
	assertGet(t, store, "key-0-0", "updated")
	assertGet(t, store, "key-4-1", "value")
}

func TestMajorCompactionGroupsUseOneWorkerPerThreeSSTs(t *testing.T) {
	dir := t.TempDir()
	if err := createRecordSegmentDirs(dir); err != nil {
		t.Fatal(err)
	}
	store := &Store{
		records:                  &segmentedRecordStore{rootDir: dir},
		majorCompactionThreadNum: 8,
	}
	for fileNo := uint64(1); fileNo <= 6; fileNo++ {
		if err := os.WriteFile(parquetSegmentPath(dir, fileNo), []byte{byte(fileNo)}, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	groups, err := store.majorCompactionSSTGroups([]uint64{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("group count for 5 SSTs = %d, want 1", len(groups))
	}

	groups, err = store.majorCompactionSSTGroups([]uint64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("group count for 6 SSTs = %d, want 2", len(groups))
	}
}

func TestMajorCompactFinalCheckCompactsSingleEmptyParquetSegment(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:            crashTestWALSize,
		MaxImmutableWALNum: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

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
	sourceWAL := store.checkpointWALFileNo
	compacted, err := store.minorCompactWAL(sourceWAL)
	if err != nil || !compacted {
		t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", sourceWAL, compacted, err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	emptySSTs := parquetFileNosForTest(store)
	if len(emptySSTs) != 1 {
		t.Fatalf("empty parquet count = %d, want 1", len(emptySSTs))
	}
	selectedSSTs, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != 1 || selectedSSTs[0] != emptySSTs[0] {
		t.Fatalf("major compaction candidates = %v, want %v", selectedSSTs, emptySSTs)
	}

	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	got := parquetFileNosForTest(store)
	if len(got) != 0 {
		t.Fatalf("parquet fileNos after empty major compaction = %v, want none", got)
	}
	if fileExistsForTest(t, parquetSegmentPath(dir, emptySSTs[0])) {
		t.Fatalf("empty parquet %d still exists after checkpoint", emptySSTs[0])
	}
	assertMissing(t, store, "alpha")
}

func TestMajorCompactionDispatcherRunsOnCompactableSST(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:       crashTestWALSize,
		TargetSSTSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	store.stopMinorCompactionDispatcher()

	keys := []string{"alpha", "bravo", "charlie"}
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
	oldSSTs := parquetFileNosForTest(store)
	if len(oldSSTs) != len(keys) {
		t.Fatalf("old parquet count = %d, want %d", len(oldSSTs), len(keys))
	}

	for _, key := range keys {
		if err := store.Put([]byte(key), []byte(key+"-updated")); err != nil {
			t.Fatal(err)
		}
	}
	waitForCompactableSSTCountForTest(t, store, 0)
	assertGet(t, store, "alpha", "alpha-updated")
	assertGet(t, store, "bravo", "bravo-updated")
	assertGet(t, store, "charlie", "charlie-updated")
}

func TestMajorCompactLimitsInputSSTCountPerRoundAndDrains(t *testing.T) {
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
	stopCompactionDispatchersForTest(store)

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
	for _, key := range keys {
		if err := store.Put([]byte(key), []byte(key+"-updated")); err != nil {
			t.Fatal(err)
		}
	}

	wantSelected := workers * majorCompactionMaxSSTsPerWorker
	allSSTs := store.records.compactableParquetFileNos()
	if len(allSSTs) != wantSelected+1 {
		t.Fatalf("compactable parquet count = %d, want %d", len(allSSTs), wantSelected+1)
	}
	selectedSSTs, hasMoreSSTs, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selectedSSTs) != wantSelected {
		t.Fatalf("major compaction candidate count = %d, want %d", len(selectedSSTs), wantSelected)
	}
	if !hasMoreSSTs {
		t.Fatalf("major compaction candidate hasMore = false, want true")
	}
	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	remainingSSTs := store.records.compactableParquetFileNos()
	if len(remainingSSTs) != 0 {
		t.Fatalf("compactable parquet after MajorCompact = %v, want none", remainingSSTs)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	for _, fileNo := range allSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after checkpoint", fileNo)
		}
	}
	for _, key := range keys {
		assertGet(t, store, key, key+"-updated")
	}
}

func TestMajorCompactionDispatcherDrainsFullGroupsFromSingleSignal(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MajorCompactionThreadNum: 1,
		TargetSSTSize:            1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

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
	oldSSTs := parquetFileNosForTest(store)
	if len(oldSSTs) != len(keys) {
		t.Fatalf("old parquet count = %d, want %d", len(oldSSTs), len(keys))
	}
	for _, key := range keys {
		if err := store.Put([]byte(key), []byte(key+"-updated")); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(store.records.compactableParquetFileNos()); got != len(keys) {
		t.Fatalf("compactable parquet count = %d, want %d", got, len(keys))
	}

	store.startMajorCompactionDispatcher()
	store.notifyMajorCompaction()
	waitForCompactableSSTCountForTest(t, store, 0)
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	for _, fileNo := range oldSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after checkpoint", fileNo)
		}
	}
	for _, key := range keys {
		assertGet(t, store, key, key+"-updated")
	}
}

func TestMajorCompactInstallSSTBatchReplaysAfterDirtyRestart(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	oldSSTs := parquetFileNosForTest(store)

	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Delete([]byte("charlie"))
	if err != nil || !deleted {
		t.Fatalf("Delete(charlie) = (%v,%v), want true,nil", deleted, err)
	}
	if err := store.Put([]byte("echo"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
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
	if len(newSSTs) != 3 {
		t.Fatalf("reopened parquet count = %d, want 3", len(newSSTs))
	}
	assertNoFileNoOverlapForTest(t, oldSSTs, newSSTs)
	for _, fileNo := range oldSSTs {
		if fileExistsForTest(t, parquetSegmentPath(dir, fileNo)) {
			t.Fatalf("old parquet %d still exists after recovery", fileNo)
		}
	}
	liveStats := manifestLiveSSTStatsForTest(t, reopened.manifest.path)
	for _, fileNo := range oldSSTs {
		if _, ok := liveStats[fileNo]; ok {
			t.Fatalf("old parquet %d still live in manifest after recovery", fileNo)
		}
	}
	for _, fileNo := range newSSTs {
		stats, ok := liveStats[fileNo]
		if !ok {
			t.Fatalf("new parquet %d missing from manifest after recovery", fileNo)
		}
		if stats.totalEntries != 1 || stats.deletedEntries != 0 {
			t.Fatalf("new parquet %d stats after recovery = total %d deleted %d, want 1,0", fileNo, stats.totalEntries, stats.deletedEntries)
		}
	}
	assertGet(t, reopened, "alpha", "updated")
	assertGet(t, reopened, "bravo", "two")
	assertMissing(t, reopened, "charlie")
	assertGet(t, reopened, "delta", "four")
	assertGet(t, reopened, "echo", "updated")
	assertGet(t, reopened, "foxtrot", "six")
}

func TestInstallSSTBatchReplaySkippedRowsCountAsDeleted(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	oldSSTs := parquetFileNosForTest(store)
	newSSTs, entries, err := store.buildMajorCompactionSSTs(oldSSTs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.records.installParquetSegments(newSSTs); err != nil {
		t.Fatal(err)
	}
	alphaNewPos := majorCompactionEntryPosForTest(t, entries, "alpha")
	alphaNewSSTFileNo := recordPositionFileNo(alphaNewPos)
	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.records.AppendInstallSSTBatchRecord(oldSSTs, parquetStoreFileNos(newSSTs)); err != nil {
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
	stopCompactionDispatchersForTest(reopened)
	defer closeForTest(t, reopened)

	assertManifestLiveSSTStatsForTest(t, reopened.manifest.path, alphaNewSSTFileNo, 1, 1)
	assertGet(t, reopened, "alpha", "updated")
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
	stopCompactionDispatchersForTest(reopened)
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
	stopCompactionDispatchersForTest(store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("junk-a"), []byte("stale-a")); err != nil {
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
	if err := store.Put([]byte("junk-b"), []byte("stale-b")); err != nil {
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

	if err := store.Put([]byte("delta"), []byte("four")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("junk-c"), []byte("stale-c")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	thirdSource := store.checkpointWALFileNo
	if compacted, err := store.minorCompactWAL(thirdSource); err != nil || !compacted {
		t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", thirdSource, compacted, err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"junk-a", "junk-b", "junk-c"} {
		deleted, err := store.Delete([]byte(key))
		if err != nil || !deleted {
			t.Fatalf("Delete(%s) = (%v,%v), want true,nil", key, deleted, err)
		}
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
	want := []string{"alpha", "beta", "delta", "gamma", "zulu"}
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
	stopCompactionDispatchersForTest(store)
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
	if err := store.Put([]byte("echo"), []byte("five")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("foxtrot"), []byte("six")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	for _, fileNo := range []uint64{firstWALSegmentNo, firstWALSegmentNo + 1, firstWALSegmentNo + 2} {
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

func majorCompactionEntryPosForTest(t *testing.T, entries []majorCompactionEntry, key string) minpatricia.Position {
	t.Helper()

	for _, entry := range entries {
		if string(entry.key) == key {
			return entry.newPos
		}
	}
	t.Fatalf("major compaction entry for %s missing", key)
	return 0
}

func waitForCompactableSSTCountForTest(t *testing.T, store *Store, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.records.compactableParquetFileNos()) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("compactable SST count = %d, want %d: %v", len(store.records.compactableParquetFileNos()), want, store.records.compactableParquetFileNos())
}
