//go:build darwin || linux

package minweight_store

import (
	"testing"
)

func TestSegmentedRecordStoreSyncClearsWALDirDirty(t *testing.T) {
	records, err := openSegmentedRecordStore(t.TempDir(), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !records.dirDirty {
		t.Fatal("new segmentedRecordStore dirDirty = false, want true")
	}
	if err := records.Sync(); err != nil {
		t.Fatal(err)
	}
	if records.dirDirty {
		t.Fatal("dirDirty after Sync = true, want false")
	}
	if _, err := records.Rollover(); err != nil {
		t.Fatal(err)
	}
	if !records.dirDirty {
		t.Fatal("dirDirty after Rollover = false, want true")
	}
	if err := records.Sync(); err != nil {
		t.Fatal(err)
	}
	if records.dirDirty {
		t.Fatal("dirDirty after rollover Sync = true, want false")
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}
}
