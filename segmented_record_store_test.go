//go:build darwin || linux

package minweight_store

import "testing"

func TestSegmentedRecordStoreSyncClearsWALDirDirty(t *testing.T) {
	dir := t.TempDir()
	if err := createRecordSegmentDirs(dir); err != nil {
		t.Fatal(err)
	}
	records, err := openSegmentedRecordStore(dir, 1<<20, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !records.walDirDirty {
		t.Fatal("new segmentedRecordStore walDirDirty = false, want true")
	}
	if err := records.Sync(); err != nil {
		t.Fatal(err)
	}
	if records.walDirDirty {
		t.Fatal("walDirDirty after Sync = true, want false")
	}
	if _, err := records.Rollover(); err != nil {
		t.Fatal(err)
	}
	if !records.walDirDirty {
		t.Fatal("walDirDirty after Rollover = false, want true")
	}
	if err := records.Sync(); err != nil {
		t.Fatal(err)
	}
	if records.walDirDirty {
		t.Fatal("walDirDirty after rollover Sync = true, want false")
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentedRecordStoreAllowsNextFileNoToSkip(t *testing.T) {
	dir := t.TempDir()
	if err := createRecordSegmentDirs(dir); err != nil {
		t.Fatal(err)
	}
	records, err := openSegmentedRecordStore(dir, 1<<20, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}

	records, err = openSegmentedRecordStore(dir, 1<<20, firstWALSegmentNo, firstWALSegmentNo+8, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, records)

	if records.nextFileNo != firstWALSegmentNo+8 {
		t.Fatalf("nextFileNo = %d, want %d", records.nextFileNo, firstWALSegmentNo+8)
	}
	old, err := records.Rollover()
	if err != nil {
		t.Fatal(err)
	}
	if old.fileNo != firstWALSegmentNo {
		t.Fatalf("old WAL file no = %d, want %d", old.fileNo, firstWALSegmentNo)
	}
	if records.activeFileNo != firstWALSegmentNo+8 {
		t.Fatalf("active WAL file no = %d, want %d", records.activeFileNo, firstWALSegmentNo+8)
	}
}
