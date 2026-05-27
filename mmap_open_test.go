//go:build darwin || linux

package minweight_store

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

func TestOpenReplaysWAL(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two-replaced")); err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.Delete([]byte("missing")); err != nil || deleted {
		t.Fatalf("Delete(missing) = (%v,%v), want false,nil", deleted, err)
	}
	if deleted, err := store.Delete([]byte("alpha")); err != nil || !deleted {
		t.Fatalf("Delete(alpha) = (%v,%v), want true,nil", deleted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	value, ok, err := store.Get([]byte("bravo"))
	if err != nil || !ok || string(value) != "two-replaced" {
		t.Fatalf("Get(bravo) = (%q,%v,%v), want two-replaced,true,nil", value, ok, err)
	}
	_, ok, err = store.Get([]byte("alpha"))
	if err != nil || ok {
		t.Fatalf("Get(alpha) ok=%v err=%v, want false,nil", ok, err)
	}
	assertItems(t, "replayed WAL Scan", store.Scan, []string{
		"bravo=two-replaced",
	})
}

func TestOpenResetsPersistedIndexBeforeReplay(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if err := store.Put([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if store.backend.nodes.LiveNodes() < 2 {
		t.Fatalf("LiveNodes after replay = %d, want multi-node index", store.backend.nodes.LiveNodes())
	}
	value, ok, err := store.Get([]byte("key-0007"))
	if err != nil || !ok || string(value) != "key-0007" {
		t.Fatalf("Get(key-0007) = (%q,%v,%v), want key-0007,true,nil", value, ok, err)
	}
}

func TestWALRecordCRC(t *testing.T) {
	wal, err := openWALRecordStore(filepath.Join(t.TempDir(), "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	pos, err := wal.Append([]byte("alpha"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	wal.data[uint64(pos)+walRecordHeaderSize] ^= 0xff
	err = wal.Replay(func(op byte, key []byte, pos minpatricia.Position) error {
		return nil
	})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Replay err = %v, want %v", err, ErrCorruptWAL)
	}
}

func TestWALFull(t *testing.T) {
	store, err := Open(t.TempDir(), Options{WALSize: walHeaderSize + walRecordHeaderSize + 8})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	err = store.Put([]byte("alpha"), []byte("value-too-large"))
	if !errors.Is(err, ErrWalFull) {
		t.Fatalf("Put err = %v, want %v", err, ErrWalFull)
	}
}

func TestInvalidKeyDoesNotAdvanceWAL(t *testing.T) {
	store, err := Open(t.TempDir(), Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	wal := store.backend.records.(*walRecordStore)
	used := wal.used
	key := make([]byte, minpatricia.MaxKeySize+1)
	err = store.Put(key, []byte("value"))
	if !errors.Is(err, minpatricia.ErrKeyTooLarge) {
		t.Fatalf("Put err = %v, want %v", err, minpatricia.ErrKeyTooLarge)
	}
	if wal.used != used {
		t.Fatalf("wal used = %d, want %d", wal.used, used)
	}
}
