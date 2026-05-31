//go:build darwin || linux

package minweight_store

import (
	"errors"
	"testing"
)

func TestOptionsDefaultMinorCompactionSettings(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	if store.minorCompactionThreadNum != 1 {
		t.Fatalf("minorCompactionThreadNum = %d, want 1", store.minorCompactionThreadNum)
	}
	if store.majorCompactionThreadNum != 1 {
		t.Fatalf("majorCompactionThreadNum = %d, want 1", store.majorCompactionThreadNum)
	}
	if store.maxImmutableWALNum != 1 {
		t.Fatalf("maxImmutableWALNum = %d, want 1", store.maxImmutableWALNum)
	}
	if store.targetSSTSize != defaultTargetSSTSize {
		t.Fatalf("targetSSTSize = %d, want %d", store.targetSSTSize, defaultTargetSSTSize)
	}
	if store.records.maxGarbageRatioPerSST != defaultMaxGarbageRatioPerSST {
		t.Fatalf("maxGarbageRatioPerSST = %v, want %v", store.records.maxGarbageRatioPerSST, defaultMaxGarbageRatioPerSST)
	}
}

func TestOptionsCustomMinorCompactionSettings(t *testing.T) {
	store, err := Open(t.TempDir(), Options{
		MinorCompactionThreadNum: 3,
		MajorCompactionThreadNum: 4,
		MaxImmutableWALNum:       5,
		TargetSSTSize:            64 << 20,
		MaxGarbageRatioPerSST:    0.3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	if store.minorCompactionThreadNum != 3 {
		t.Fatalf("minorCompactionThreadNum = %d, want 3", store.minorCompactionThreadNum)
	}
	if store.majorCompactionThreadNum != 4 {
		t.Fatalf("majorCompactionThreadNum = %d, want 4", store.majorCompactionThreadNum)
	}
	if store.maxImmutableWALNum != 5 {
		t.Fatalf("maxImmutableWALNum = %d, want 5", store.maxImmutableWALNum)
	}
	if store.targetSSTSize != 64<<20 {
		t.Fatalf("targetSSTSize = %d, want %d", store.targetSSTSize, int64(64<<20))
	}
	if store.records.maxGarbageRatioPerSST != 0.3 {
		t.Fatalf("maxGarbageRatioPerSST = %v, want 0.3", store.records.maxGarbageRatioPerSST)
	}
}

func TestOptionsRejectNegativeMinorCompactionSettings(t *testing.T) {
	if _, err := Open(t.TempDir(), Options{MinorCompactionThreadNum: -1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative minor compaction threads err = %v, want %v", err, ErrOptions)
	}
	if _, err := Open(t.TempDir(), Options{MajorCompactionThreadNum: -1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative major compaction threads err = %v, want %v", err, ErrOptions)
	}
	if _, err := Open(t.TempDir(), Options{MaxImmutableWALNum: -1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative max immutable wal err = %v, want %v", err, ErrOptions)
	}
	if _, err := Open(t.TempDir(), Options{TargetSSTSize: -1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative target sst size err = %v, want %v", err, ErrOptions)
	}
	if _, err := Open(t.TempDir(), Options{MaxGarbageRatioPerSST: -0.1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative max garbage ratio err = %v, want %v", err, ErrOptions)
	}
	if _, err := Open(t.TempDir(), Options{MaxGarbageRatioPerSST: 1.1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open too-large max garbage ratio err = %v, want %v", err, ErrOptions)
	}
}
