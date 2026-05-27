//go:build darwin || linux

package minweight_store

import (
	"errors"
	"fmt"
	"os"
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

func TestOpenCleanManifestSkipsReplay(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, manifestName)); err != nil {
		t.Fatal(err)
	}

	wal, err := openMmapWALRecordStore(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	wal.data[walHeaderSize+walRecordCRCOffset] ^= 0xff
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertGet(t, store, "alpha", "one")
	if _, err := os.Stat(filepath.Join(dir, manifestName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest after Open err = %v, want not exist", err)
	}
}

func TestOpenDirtyStoreReplaysWAL(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	backend := store.backend
	store.backend = nil
	if err := backend.syncAndClose(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
}

func TestOpenRejectsCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(dir, manifestName)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	data[manifestWALUsedOffset] ^= 0xff
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{WALSize: 1 << 20})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("Open err = %v, want %v", err, ErrManifest)
	}
}

func TestWALRecordCRC(t *testing.T) {
	wal, err := openMmapWALRecordStore(filepath.Join(t.TempDir(), "wal"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	pos, err := wal.Append([]byte("alpha"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	wal.data[uint64(pos)+walRecordHeaderSize] ^= 0xff
	err = wal.Replay(WALReplayStrict, func(op byte, key []byte, pos minpatricia.Position) error {
		return nil
	})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Replay err = %v, want %v", err, ErrCorruptWAL)
	}
}

func TestOpenStrictRejectsCorruptWAL(t *testing.T) {
	dir, walSize, _, _ := corruptMiddleWAL(t)
	_, err := Open(dir, Options{WALSize: walSize})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Open err = %v, want %v", err, ErrCorruptWAL)
	}
}

func TestOpenPointInTimeTruncatesCorruptWAL(t *testing.T) {
	dir, walSize, corruptPos, _ := corruptMiddleWAL(t)
	store, err := Open(dir, Options{
		WALSize:         walSize,
		WALReplayPolicy: WALReplayPointInTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	assertGet(t, store, "alpha", "one")
	assertMissing(t, store, "bravo")
	assertMissing(t, store, "charlie")

	wal := store.wal
	if wal.used != uint64(corruptPos) {
		t.Fatalf("wal used = %d, want %d", wal.used, corruptPos)
	}
}

func TestOpenBestEffortSkipsCorruptWALRecord(t *testing.T) {
	dir, walSize, _, used := corruptMiddleWAL(t)
	store, err := Open(dir, Options{
		WALSize:         walSize,
		WALReplayPolicy: WALReplayBestEffort,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	assertGet(t, store, "alpha", "one")
	assertMissing(t, store, "bravo")
	assertGet(t, store, "charlie", "three")

	wal := store.wal
	if wal.used != used {
		t.Fatalf("wal used = %d, want unchanged %d", wal.used, used)
	}
}

func TestOpenRejectsInvalidWALReplayPolicy(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{WALSize: 1 << 20, WALReplayPolicy: WALReplayPolicy(99)})
	if !errors.Is(err, ErrReplayPolicy) {
		t.Fatalf("Open err = %v, want %v", err, ErrReplayPolicy)
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

	wal := store.wal
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

func corruptMiddleWAL(t *testing.T) (string, int64, minpatricia.Position, uint64) {
	t.Helper()

	const walSize = int64(1 << 20)
	dir := t.TempDir()
	wal, err := openMmapWALRecordStore(filepath.Join(dir, "wal"), walSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	corruptPos, err := wal.Append([]byte("bravo"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append([]byte("charlie"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	wal.data[uint64(corruptPos)+walRecordHeaderSize] ^= 0xff
	used := wal.used
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, walSize, corruptPos, used
}

func assertGet(t *testing.T, store *Store, key, want string) {
	t.Helper()

	value, ok, err := store.Get([]byte(key))
	if err != nil || !ok || string(value) != want {
		t.Fatalf("Get(%s) = (%q,%v,%v), want %s,true,nil", key, value, ok, err, want)
	}
}

func assertMissing(t *testing.T, store *Store, key string) {
	t.Helper()

	_, ok, err := store.Get([]byte(key))
	if err != nil || ok {
		t.Fatalf("Get(%s) ok=%v err=%v, want false,nil", key, ok, err)
	}
}
