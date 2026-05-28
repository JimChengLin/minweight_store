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
	defer closeForTest(t, store)

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
	defer closeForTest(t, store)

	if store.backend.nodes.LiveNodes() < 2 {
		t.Fatalf("LiveNodes after replay = %d, want multi-node index", store.backend.nodes.LiveNodes())
	}
	value, ok, err := store.Get([]byte("key-0007"))
	if err != nil || !ok || string(value) != "key-0007" {
		t.Fatalf("Get(key-0007) = (%q,%v,%v), want key-0007,true,nil", value, ok, err)
	}
}

func TestOpenGracefulShutdownSkipsReplay(t *testing.T) {
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
	state, hasLegalManifest, err := (&manifest{path: filepath.Join(dir, manifestName)}).read()
	if err != nil {
		t.Fatal(err)
	}
	if !hasLegalManifest {
		t.Fatal("manifest is missing after graceful shutdown")
	}
	if state.checkpointWALFileNo != firstWALSegmentNo || state.activeWALFileNo != firstWALSegmentNo+1 {
		t.Fatalf("manifest state = %+v, want checkpoint=%d active=%d", state, firstWALSegmentNo, firstWALSegmentNo+1)
	}
	cleanWAL, err := openMmapWALRecordStore(filepath.Join(walSegmentsPath(dir), walSegmentName(firstWALSegmentNo+1)), 1<<20, firstWALSegmentNo+1)
	if err != nil {
		t.Fatal(err)
	}
	if cleanWAL.used != walHeaderSize {
		t.Fatalf("clean WAL used = %d, want %d", cleanWAL.used, walHeaderSize)
	}
	if err := cleanWAL.Close(); err != nil {
		t.Fatal(err)
	}

	wal, err := openMmapWALRecordStore(filepath.Join(walSegmentsPath(dir), walSegmentName(firstWALSegmentNo)), 1<<20, firstWALSegmentNo)
	if err != nil {
		t.Fatal(err)
	}
	wal.data[walHeaderSize+walRecordCRCOffset] ^= 0xff
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(secondaryIndexPath(dir)); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	if _, err := os.Stat(filepath.Join(dir, manifestName)); err != nil {
		t.Fatalf("manifest after Open err = %v, want exists", err)
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
	store.records = nil
	store.backend = nil
	if err := backend.syncAndClose(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	if store.checkpointWALFileNo != firstWALSegmentNo+1 {
		t.Fatalf("checkpoint WAL file no = %d, want %d", store.checkpointWALFileNo, firstWALSegmentNo+1)
	}
	if store.records.activeFileNo != firstWALSegmentNo+2 {
		t.Fatalf("active WAL file no = %d, want %d", store.records.activeFileNo, firstWALSegmentNo+2)
	}

	state, hasLegalManifest, err := store.manifest.read()
	if err != nil {
		t.Fatal(err)
	}
	if !hasLegalManifest {
		t.Fatal("manifest is missing after startup checkpoint")
	}
	if state.checkpointWALFileNo != firstWALSegmentNo+1 || state.activeWALFileNo != firstWALSegmentNo+2 {
		t.Fatalf("manifest state = %+v, want checkpoint=%d active=%d", state, firstWALSegmentNo+1, firstWALSegmentNo+2)
	}
}

func TestOpenLegalManifestDropsEmptyRolloverWAL(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	wal3, err := openMmapWALRecordStore(filepath.Join(walSegmentsPath(dir), walSegmentName(firstWALSegmentNo+2)), walSize, firstWALSegmentNo+2)
	if err != nil {
		t.Fatal(err)
	}
	if wal3.used != walHeaderSize {
		t.Fatalf("wal3 used = %d, want %d", wal3.used, walHeaderSize)
	}
	if err := wal3.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	if store.checkpointWALFileNo != firstWALSegmentNo+1 {
		t.Fatalf("checkpoint WAL file no = %d, want %d", store.checkpointWALFileNo, firstWALSegmentNo+1)
	}
	if store.records.activeFileNo != firstWALSegmentNo+2 {
		t.Fatalf("active WAL file no = %d, want %d", store.records.activeFileNo, firstWALSegmentNo+2)
	}
}

func TestOpenPrimaryWALFlushedManifestTrustsPrimary(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	simulatePrimaryWALFlushedCheckpointForTest(t, store)
	if err := os.RemoveAll(secondaryIndexPath(dir)); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	state, hasLegalManifest, err := store.manifest.read()
	if err != nil {
		t.Fatal(err)
	}
	if !hasLegalManifest {
		t.Fatal("manifest is missing after primary-flushed recovery")
	}
	if state.primaryWALFlushed {
		t.Fatal("primaryWALFlushed = true, want false")
	}
	if state.checkpointWALFileNo != firstWALSegmentNo+1 || state.activeWALFileNo != firstWALSegmentNo+2 {
		t.Fatalf("manifest state = %+v, want checkpoint=%d active=%d", state, firstWALSegmentNo+1, firstWALSegmentNo+2)
	}

	if err := store.Put([]byte("charlie"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	assertGet(t, store, "charlie", "three")
}

func TestOpenPrimaryWALFlushedManifestAfterSecondaryReplayTrustsPrimary(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	simulateCheckpointAfterSecondaryReplayBeforeManifestForTest(t, store)

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	state, hasLegalManifest, err := store.manifest.read()
	if err != nil {
		t.Fatal(err)
	}
	if !hasLegalManifest {
		t.Fatal("manifest is missing after primary-flushed recovery")
	}
	if state.primaryWALFlushed {
		t.Fatal("primaryWALFlushed = true, want false")
	}
}

func TestOpenPrimaryWALFlushedRejectsNonEmptyActiveWAL(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	simulatePrimaryWALFlushedCheckpointForTest(t, store)

	wal, err := openMmapWALRecordStore(filepath.Join(walSegmentsPath(dir), walSegmentName(firstWALSegmentNo+2)), walSize, firstWALSegmentNo+2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append([]byte("charlie"), []byte("three")); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{WALSize: walSize})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("Open err = %v, want %v", err, ErrManifest)
	}
}

func TestOpenDirtyManifestRequiresSecondaryCheckpoint(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)
	if err := os.RemoveAll(secondaryIndexPath(dir)); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{WALSize: walSize})
	if err == nil {
		t.Fatal("Open err = nil, want failure without secondary checkpoint")
	}
}

func TestOpenPrimaryWALFlushedRecoveryCleansSecondaryCopyTemp(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	simulatePrimaryWALFlushedCheckpointForTest(t, store)

	temp := filepath.Join(secondaryIndexPath(dir), mmapNodeExtentName(0)+mmapNodeExtentCopyTempSuffix)
	if err := os.WriteFile(temp, []byte("stale temp"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertGet(t, store, "bravo", "two")
	if _, err := os.Stat(temp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secondary copy temp stat err = %v, want %v", err, os.ErrNotExist)
	}
}

func TestOpenRejectsCorruptManifest(t *testing.T) {
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

	manifestPath := filepath.Join(dir, manifestName)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset+manifestRecordSize <= len(data); offset += manifestRecordSize {
		data[offset+manifestVersionOffset] ^= 0xff
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{WALSize: 1 << 20})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("Open err = %v, want %v", err, ErrManifest)
	}
}

func TestOpenExplicitWALSizeDoesNotRejectDifferentManifestSize(t *testing.T) {
	dir := t.TempDir()
	oldSize := int64(walHeaderSize + walRecordHeaderSize + len("alpha") + len("one"))
	store, err := Open(dir, Options{WALSize: oldSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	newSize := oldSize + 128
	store, err = Open(dir, Options{WALSize: newSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	if store.records.size != newSize {
		t.Fatalf("WALSize = %d, want explicit %d", store.records.size, newSize)
	}
	assertGet(t, store, "alpha", "one")
}

func TestWALRecordCRC(t *testing.T) {
	wal, err := openMmapWALRecordStore(filepath.Join(t.TempDir(), walSegmentName(firstWALSegmentNo)), 1<<20, firstWALSegmentNo)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, wal)

	pos, err := wal.Append([]byte("alpha"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	wal.data[recordPositionOffset(pos)+walRecordHeaderSize] ^= 0xff
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
	defer closeForTest(t, store)

	assertGet(t, store, "alpha", "one")
	assertMissing(t, store, "bravo")
	assertMissing(t, store, "charlie")

	wal := store.records.activeSegment()
	if wal.used != recordPositionOffset(corruptPos) {
		t.Fatalf("wal used = %d, want %d", wal.used, recordPositionOffset(corruptPos))
	}
}

func TestOpenRejectsNoManifestMultipleWALSegments(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	walDir := walSegmentsPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}

	wal1, err := openMmapWALRecordStore(filepath.Join(walDir, walSegmentName(1)), walSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal1.Append([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	corruptPos, err := wal1.Append([]byte("bravo"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	wal1.data[recordPositionOffset(corruptPos)+walRecordHeaderSize] ^= 0xff
	if err := wal1.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := openMmapWALRecordStore(filepath.Join(walDir, walSegmentName(2)), walSize, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal2.Append([]byte("charlie"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := wal2.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{
		WALSize:         walSize,
		WALReplayPolicy: WALReplayPointInTime,
	})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("Open err = %v, want %v", err, ErrManifest)
	}
}

func TestOpenNoManifestDropsEmptyRolloverWAL(t *testing.T) {
	const walSize = int64(1 << 20)
	dir := t.TempDir()
	walDir := walSegmentsPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}

	wal1, err := openMmapWALRecordStore(filepath.Join(walDir, walSegmentName(1)), walSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal1.Append([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := wal1.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := openMmapWALRecordStore(filepath.Join(walDir, walSegmentName(2)), walSize, 2)
	if err != nil {
		t.Fatal(err)
	}
	if wal2.used != walHeaderSize {
		t.Fatalf("wal2 used = %d, want %d", wal2.used, walHeaderSize)
	}
	if err := wal2.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

	assertGet(t, store, "alpha", "one")
	if store.records.activeFileNo != firstWALSegmentNo {
		t.Fatalf("active WAL file no = %d, want %d", store.records.activeFileNo, firstWALSegmentNo)
	}
	if store.records.nextFileNo != firstWALSegmentNo+1 {
		t.Fatalf("next WAL file no = %d, want %d", store.records.nextFileNo, firstWALSegmentNo+1)
	}
	if _, err := os.Stat(filepath.Join(walDir, walSegmentName(2))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty rollover WAL stat err = %v, want %v", err, os.ErrNotExist)
	}
}

func TestOpenBestEffortRepairsCorruptWALRecord(t *testing.T) {
	dir, walSize, _, used := corruptMiddleWAL(t)
	store, err := Open(dir, Options{
		WALSize:         walSize,
		WALReplayPolicy: WALReplayBestEffort,
	})
	if err != nil {
		t.Fatal(err)
	}

	assertGet(t, store, "alpha", "one")
	assertMissing(t, store, "bravo")
	assertGet(t, store, "charlie", "three")

	wal := store.records.activeSegment()
	if wal.used >= used {
		t.Fatalf("wal used = %d, want best-effort repair below old used %d", wal.used, used)
	}
	if err := wal.Replay(WALReplayStrict, func(op byte, key []byte, pos minpatricia.Position) error {
		return nil
	}); err != nil {
		t.Fatalf("strict replay after best-effort repair err = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("delta"), []byte("four")); err != nil {
		t.Fatal(err)
	}
	backend := store.backend
	store.records = nil
	store.backend = nil
	if err := backend.syncAndClose(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	assertGet(t, store, "alpha", "one")
	assertMissing(t, store, "bravo")
	assertGet(t, store, "charlie", "three")
	assertGet(t, store, "delta", "four")
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

func TestOpenRejectsWALSizeLargerThanRecordOffsetLimit(t *testing.T) {
	_, err := Open(t.TempDir(), Options{WALSize: int64(recordOffsetLimit) + 1})
	if !errors.Is(err, ErrWalFull) {
		t.Fatalf("Open err = %v, want %v", err, ErrWalFull)
	}
}

func TestWALFull(t *testing.T) {
	store, err := Open(t.TempDir(), Options{WALSize: walHeaderSize + walRecordHeaderSize + 8})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)

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
	defer closeForTest(t, store)

	wal := store.records.activeSegment()
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
	walDir := walSegmentsPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wal, err := openMmapWALRecordStore(filepath.Join(walDir, walSegmentName(firstWALSegmentNo)), walSize, firstWALSegmentNo)
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
	wal.data[recordPositionOffset(corruptPos)+walRecordHeaderSize] ^= 0xff
	used := wal.used
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, walSize, corruptPos, used
}

func dirtySyncAndCloseStoreForTest(t *testing.T, store *Store) {
	t.Helper()

	backend := store.backend
	store.records = nil
	store.backend = nil
	if err := backend.syncAndClose(); err != nil {
		t.Fatal(err)
	}
}

func simulatePrimaryWALFlushedCheckpointForTest(t *testing.T, store *Store) {
	t.Helper()

	oldWALFileNo := store.records.activeFileNo
	oldWAL, err := store.records.Rollover()
	if err != nil {
		t.Fatal(err)
	}
	active := store.records.activeSegment()
	if err := syncPrimaryIndexAndWAL(store.backend, oldWAL); err != nil {
		t.Fatal(err)
	}
	if err := active.Sync(); err != nil {
		t.Fatal(err)
	}
	state := manifestState{
		checkpointWALFileNo: oldWALFileNo,
		activeWALFileNo:     store.records.activeFileNo,
		nextWALFileNo:       store.records.nextFileNo,
		walSegmentSize:      uint64(store.records.size),
		primaryWALFlushed:   true,
	}
	if err := store.manifest.write(state); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)
}

func simulateCheckpointAfterSecondaryReplayBeforeManifestForTest(t *testing.T, store *Store) {
	t.Helper()

	oldWALFileNo := store.records.activeFileNo
	oldWAL, err := store.records.Rollover()
	if err != nil {
		t.Fatal(err)
	}
	active := store.records.activeSegment()
	if err := syncPrimaryIndexAndWAL(store.backend, oldWAL); err != nil {
		t.Fatal(err)
	}
	if err := active.Sync(); err != nil {
		t.Fatal(err)
	}
	state := manifestState{
		checkpointWALFileNo: oldWALFileNo,
		activeWALFileNo:     store.records.activeFileNo,
		nextWALFileNo:       store.records.nextFileNo,
		walSegmentSize:      uint64(store.records.size),
		primaryWALFlushed:   true,
	}
	if err := store.manifest.write(state); err != nil {
		t.Fatal(err)
	}
	if err := checkpointSecondaryIndex(store.manifest.dir(), store.records, oldWALFileNo, WALReplayStrict); err != nil {
		t.Fatal(err)
	}
	dirtySyncAndCloseStoreForTest(t, store)
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
