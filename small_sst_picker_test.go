package minweight_store

import (
	"testing"
	"time"
)

func TestPickSmallestSSTsUsesPhysicalSizeThenFileNumber(t *testing.T) {
	candidates := []parquetSSTCandidate{
		{fileNo: 9, bytes: 5}, {fileNo: 3, bytes: 2}, {fileNo: 2, bytes: 2},
		{fileNo: 7, bytes: 3}, {fileNo: 5, bytes: 4},
	}
	fileNos := pickSmallestSSTs(candidates, 10)
	if !sameFileNos(fileNos, []uint64{2, 3, 7, 5}) {
		t.Fatalf("picked SSTs = %v, want [2 3 7 5]", fileNos)
	}
}

func sameFileNos(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func TestPickSmallestSSTsRequiresFour(t *testing.T) {
	if got := pickSmallestSSTs([]parquetSSTCandidate{{fileNo: 1}, {fileNo: 2}, {fileNo: 3}}, 1); len(got) != 0 {
		t.Fatalf("picked SSTs = %v, want none", got)
	}
}

func TestMajorCompactPicksLiveSmallSSTs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	stopCompactionDispatchersForTest(store)

	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for _, key := range keys {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := store.flush(); err != nil {
			t.Fatal(err)
		}
		sourceWAL := store.checkpointWALFileNo
		if compacted, err := store.minorCompactWAL(sourceWAL); err != nil || !compacted {
			t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", sourceWAL, compacted, err)
		}
		if err := store.flush(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if _, ok, err := store.Get([]byte(key)); err == nil && ok {
				store.primaryMu.RLock()
				pos, found, lookupErr := store.backend.index.Get([]byte(key))
				store.primaryMu.RUnlock()
				if lookupErr != nil {
					t.Fatal(lookupErr)
				}
				if found && recordPositionFileNo(pos) != firstWALSegmentNo {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MajorCompactionThreadNum: 0,
		TargetSSTSize:            1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	selected, _, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) < majorCompactionSmallSSTsPerGroup {
		t.Fatalf("selected SSTs = %v, want at least four", selected)
	}
	candidates, err := store.records.liveParquetSSTsBelow(store.targetSSTSize / majorCompactionSmallSSTsPerGroup)
	if err != nil {
		t.Fatal(err)
	}
	if want := pickSmallestSSTs(candidates, store.targetSSTSize); !sameFileNos(selected, want) {
		t.Fatalf("selected SSTs = %v, want picker order %v", selected, want)
	}
	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		assertGet(t, store, key, "value")
	}
}

func TestMinorDispatcherWakesMajorForCleanSmallSSTs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MinorCompactionThreadNum: 1,
		MajorCompactionThreadNum: 1,
		MaxImmutableWALNum:       0,
		TargetSSTSize:            1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	store.stopMinorCompactionDispatcher()
	store.stopMajorCompactionDispatcher()

	keys := []string{"alpha", "bravo", "charlie", "delta"}
	for _, key := range keys {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := store.flush(); err != nil {
			t.Fatal(err)
		}
	}
	store.startMajorCompactionDispatcher()
	if err := store.minorCompact(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(parquetFileNosForTest(store)); got < majorCompactionSmallSSTsPerGroup {
			for _, key := range keys {
				assertGet(t, store, key, "value")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("clean small SSTs were not automatically compacted below the picker threshold: %v", parquetFileNosForTest(store))
}
