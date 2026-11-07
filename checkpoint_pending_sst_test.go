//go:build darwin || linux

package minweight_store

import "testing"

func TestCheckpointReplayDoesNotRequeueRetiredSST(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

	if compacted, err := store.minorCompactWAL(firstWALSegmentNo); err != nil || !compacted {
		t.Fatalf("minorCompactWAL = (%v, %v)", compacted, err)
	}
	intermediateSST := onlyParquetFileNoForTest(t, store)
	newSSTs, entries, err := store.buildMajorCompactionSSTs([]uint64{intermediateSST})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.records.installParquetSegments(newSSTs); err != nil {
		t.Fatal(err)
	}
	if err := store.publishInstalledSSTBatch([]uint64{intermediateSST}, newSSTs, entries); err != nil {
		t.Fatal(err)
	}
	if candidates := store.records.compactableParquetFileNos(); len(candidates) != 0 {
		t.Fatalf("retired SST initially remained a candidate: %v", candidates)
	}

	// Checkpoint replay visits the original minor install before the major
	// install. Its SST is already retired in the live primary state.
	if err := store.records.markSSTLive(intermediateSST); err != nil {
		t.Fatal(err)
	}
	if candidates := store.records.compactableParquetFileNos(); len(candidates) != 0 {
		t.Fatalf("checkpoint replay requeued retired SST: %v", candidates)
	}
}

func TestCheckpointReplayCannotPublishReclaimedSST(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	stopCompactionDispatchersForTest(store)
	defer closeForTest(t, store)

	var sourceWALs []uint64
	for _, key := range []string{"alpha", "bravo", "charlie"} {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := store.flush(); err != nil {
			t.Fatal(err)
		}
		sourceWALs = append(sourceWALs, store.checkpointWALFileNo)
	}
	for _, fileNo := range sourceWALs {
		if compacted, err := store.minorCompactWAL(fileNo); err != nil || !compacted {
			t.Fatalf("minorCompactWAL = (%v, %v)", compacted, err)
		}
	}
	oldSSTs := parquetFileNosForTest(store)
	newSSTs, entries, err := store.buildMajorCompactionSSTs(oldSSTs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.records.installParquetSegments(newSSTs); err != nil {
		t.Fatal(err)
	}
	if err := store.publishInstalledSSTBatch(oldSSTs, newSSTs, entries); err != nil {
		t.Fatal(err)
	}

	// Pause the compactor after selection/build while checkpoint replay is
	// still visiting intermediate installs. Resume publication only after
	// the checkpoint has replayed the final install and reclaimed the SSTs.
	var selected []uint64
	store.records.onCompactableFileAdded = func() {
		if selected != nil {
			return
		}
		fileNos, _, err := store.majorCompactionSSTFileNos()
		if err != nil {
			t.Fatal(err)
		}
		if len(fileNos) != len(oldSSTs) {
			return
		}
		selected = fileNos
		staleSSTs, staleEntries, err := store.buildMajorCompactionSSTs(fileNos)
		if err != nil || len(staleSSTs) != 0 || len(staleEntries) != 0 {
			t.Fatalf("retired SST build = (%d, %d, %v)", len(staleSSTs), len(staleEntries), err)
		}
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	store.records.onCompactableFileAdded = nil
	if selected != nil {
		err := store.publishInstalledSSTBatch(selected, nil, nil)
		t.Fatalf("checkpoint exposed retired SSTs %v; publication after reclamation: %v", selected, err)
	}
	assertGet(t, store, "alpha", "value")
	assertGet(t, store, "bravo", "value")
	assertGet(t, store, "charlie", "value")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{WALSize: crashTestWALSize, WALReplayPolicy: WALReplayStrict})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, reopened)
	assertGet(t, reopened, "alpha", "value")
	assertGet(t, reopened, "bravo", "value")
	assertGet(t, reopened, "charlie", "value")
}
