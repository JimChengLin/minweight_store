//go:build darwin || linux

package minweight_store

import (
	"testing"

	"github.com/JimChengLin/minpatricia"
)

func TestCheckpointReplaysMinorThenMajorCompactionChain(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	stopCompactionDispatchersForTest(store)

	compacted, err := store.minorCompactWAL(firstWALSegmentNo)
	if err != nil || !compacted {
		t.Fatalf("minorCompactWAL = (%v,%v), want true,nil", compacted, err)
	}
	var intermediateSST uint64
	for fileNo := range store.records.liveSSTs {
		intermediateSST = fileNo
	}
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
	if err := store.Put([]byte("trigger"), []byte("flush")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	nodes, err := openMmapNodeStore(secondaryIndexPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	secondary, err := minpatricia.OpenWithNodes(store.records, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondary.Ascend(func([]byte, minpatricia.Position) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
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
}
