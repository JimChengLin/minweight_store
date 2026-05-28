//go:build darwin || linux

package minweight_store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFlushCheckpointsSecondaryWithoutSwitchingLiveIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	primaryNodes := store.backend.nodes
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}

	if store.backend.nodes != primaryNodes {
		t.Fatal("Flush switched the live primary index")
	}
	if store.checkpointWALFileNo != firstWALSegmentNo {
		t.Fatalf("checkpoint WAL file no = %d, want %d", store.checkpointWALFileNo, firstWALSegmentNo)
	}
	if store.records.activeFileNo != firstWALSegmentNo+1 {
		t.Fatalf("active WAL file no = %d, want %d", store.records.activeFileNo, firstWALSegmentNo+1)
	}
	if _, err := os.Stat(filepath.Join(dir, manifestName)); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "alpha", "one")
}

func TestWALFullFlushesAndRollsToNewSegment(t *testing.T) {
	dir := t.TempDir()
	walSize := int64(walHeaderSize + walRecordHeaderSize + len("alpha") + len("one"))
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	if store.checkpointWALFileNo != firstWALSegmentNo {
		t.Fatalf("checkpoint WAL file no = %d, want %d", store.checkpointWALFileNo, firstWALSegmentNo)
	}
	if store.records.activeFileNo != firstWALSegmentNo+1 {
		t.Fatalf("active WAL file no = %d, want %d", store.records.activeFileNo, firstWALSegmentNo+1)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
}

func TestFlushAfterBestEffortOpenUsesRepairedWAL(t *testing.T) {
	dir, walSize, _, _ := corruptMiddleWAL(t)
	store, err := Open(dir, Options{
		WALSize:         walSize,
		WALReplayPolicy: WALReplayBestEffort,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "alpha", "one")
	assertMissing(t, store, "bravo")
	assertGet(t, store, "charlie", "three")
	if store.checkpointWALFileNo != firstWALSegmentNo {
		t.Fatalf("checkpoint WAL file no = %d, want %d", store.checkpointWALFileNo, firstWALSegmentNo)
	}
}

func TestFlushDoesNotWaitForReader(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}

	readerEntered := make(chan struct{})
	releaseReader := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		readerDone <- store.Scan(func(Item) bool {
			close(readerEntered)
			<-releaseReader
			return false
		})
	}()

	<-readerEntered
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- store.flush()
	}()

	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(releaseReader)
		t.Fatal("Flush waited for an active reader")
	}

	close(releaseReader)
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
}
