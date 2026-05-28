//go:build darwin || linux

package minweight_store

import (
	"path/filepath"
	"testing"
)

func TestMmapWALRecordStoreSyncClearsMetadataDirty(t *testing.T) {
	path := filepath.Join(t.TempDir(), walSegmentName(firstWALSegmentNo))
	wal, err := openMmapWALRecordStore(path, 1<<20, firstWALSegmentNo)
	if err != nil {
		t.Fatal(err)
	}
	if !wal.metadataDirty {
		t.Fatal("new WAL metadataDirty = false, want true")
	}
	if _, err := wal.Append([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if wal.metadataDirty {
		t.Fatal("metadataDirty after Sync = true, want false")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal, err = openMmapWALRecordStore(path, 1<<20, firstWALSegmentNo)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, wal)
	if wal.metadataDirty {
		t.Fatal("reopened WAL metadataDirty = true, want false")
	}
}
