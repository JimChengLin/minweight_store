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
	if store.maxImmutableWALNum != 1 {
		t.Fatalf("maxImmutableWALNum = %d, want 1", store.maxImmutableWALNum)
	}
}

func TestOptionsCustomMinorCompactionSettings(t *testing.T) {
	store, err := Open(t.TempDir(), Options{
		MinorCompactionThreadNum: 3,
		MaxImmutableWALNum:       5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	if store.minorCompactionThreadNum != 3 {
		t.Fatalf("minorCompactionThreadNum = %d, want 3", store.minorCompactionThreadNum)
	}
	if store.maxImmutableWALNum != 5 {
		t.Fatalf("maxImmutableWALNum = %d, want 5", store.maxImmutableWALNum)
	}
}

func TestOptionsRejectNegativeMinorCompactionSettings(t *testing.T) {
	if _, err := Open(t.TempDir(), Options{MinorCompactionThreadNum: -1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative minor compaction threads err = %v, want %v", err, ErrOptions)
	}
	if _, err := Open(t.TempDir(), Options{MaxImmutableWALNum: -1}); !errors.Is(err, ErrOptions) {
		t.Fatalf("Open negative max immutable wal err = %v, want %v", err, ErrOptions)
	}
}
