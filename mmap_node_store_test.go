//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

func TestMmapNodeStoreAllocFree(t *testing.T) {
	nodes, err := openMmapNodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)

	if nodes.LiveNodes() != 1 {
		t.Fatalf("LiveNodes after open = %d, want 1", nodes.LiveNodes())
	}
	if _, err := nodes.Get(nodes.Root()); err != nil {
		t.Fatalf("Get(root): %v", err)
	}

	id, _, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("first allocated id = %d, want 1", id)
	}
	if nodes.LiveNodes() != 2 {
		t.Fatalf("LiveNodes after alloc = %d, want 2", nodes.LiveNodes())
	}

	if err := nodes.Free(id); err != nil {
		t.Fatal(err)
	}
	if nodes.LiveNodes() != 1 {
		t.Fatalf("LiveNodes after free = %d, want 1", nodes.LiveNodes())
	}
	if _, err := nodes.Get(id); err == nil {
		t.Fatalf("Get(%d) after free succeeded", id)
	}

	reused, _, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if reused != id {
		t.Fatalf("reused id = %d, want %d", reused, id)
	}
}

func TestMmapNodeStoreDoesNotClearReusedPage(t *testing.T) {
	nodes, err := openMmapNodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)

	id, page, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte("caller owns page clearing")
	copy(mmapNodePageBytes(page)[123:], marker)

	if err := nodes.Free(id); err != nil {
		t.Fatal(err)
	}
	reused, page, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if reused != id {
		t.Fatalf("reused id = %d, want %d", reused, id)
	}
	got := mmapNodePageBytes(page)[123 : 123+len(marker)]
	if !bytes.Equal(got, marker) {
		t.Fatalf("reused page marker = %q, want %q", got, marker)
	}
}

func TestMmapNodeStoreCreatesMultipleExtents(t *testing.T) {
	dir := t.TempDir()
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)

	var last uint64
	for i := 0; i < mmapNodeSlotsPerExtent; i++ {
		id, _, err := nodes.Alloc()
		if err != nil {
			t.Fatal(err)
		}
		last = id
	}
	if last != mmapNodeSlotsPerExtent {
		t.Fatalf("last allocated id = %d, want %d", last, mmapNodeSlotsPerExtent)
	}
	if len(nodes.extents) != 2 {
		t.Fatalf("extent count = %d, want 2", len(nodes.extents))
	}

	info, err := os.Stat(filepath.Join(dir, mmapNodeExtentName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != mmapNodeExtentBytes {
		t.Fatalf("extent size = %d, want %d", info.Size(), mmapNodeExtentBytes)
	}
}

func TestMmapNodeStoreAllocSkipsFullBitmapBytes(t *testing.T) {
	nodes, err := openMmapNodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)

	extent := nodes.extents[0]
	bitmap := extent.bitmap()
	for byteIndex := 0; byteIndex < mmapNodeBitmapBytes; byteIndex++ {
		bitmap[byteIndex] = bitsetByteMask(byteIndex, mmapNodeSlotsPerExtent)
	}
	lastSlot := uint64(mmapNodeSlotsPerExtent - 1)
	extent.setUsed(lastSlot, false)
	extent.setLiveSlots(mmapNodeSlotsPerExtent - 1)

	id, _, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if id != lastSlot {
		t.Fatalf("allocated id = %d, want %d", id, lastSlot)
	}
}

func TestMmapNodeStoreReopen(t *testing.T) {
	dir := t.TempDir()
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, page, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte("persisted page bytes")
	copy(mmapNodePageBytes(page)[321:], marker)
	if err := nodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		t.Fatal(err)
	}

	nodes, err = openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)

	if nodes.LiveNodes() != 2 {
		t.Fatalf("LiveNodes after reopen = %d, want 2", nodes.LiveNodes())
	}
	page, err = nodes.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	got := mmapNodePageBytes(page)[321 : 321+len(marker)]
	if !bytes.Equal(got, marker) {
		t.Fatalf("reopened page marker = %q, want %q", got, marker)
	}
}

func TestMmapNodeStoreSyncClearsExtentMetadataDirty(t *testing.T) {
	dir := t.TempDir()
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !nodes.extents[0].metadataDirty {
		t.Fatal("new extent metadataDirty = false, want true")
	}
	if err := nodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if nodes.extents[0].metadataDirty {
		t.Fatal("metadataDirty after Sync = true, want false")
	}
	if err := nodes.Close(); err != nil {
		t.Fatal(err)
	}

	nodes, err = openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)
	if nodes.extents[0].metadataDirty {
		t.Fatal("reopened extent metadataDirty = true, want false")
	}
}

func TestMmapNodeStoreSparseCopySkipsUnusedPages(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	nodes, err := openMmapNodeStore(src)
	if err != nil {
		t.Fatal(err)
	}
	id, page, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	copy(mmapNodePageBytes(page)[321:], []byte("used page marker"))
	if err := nodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		t.Fatal(err)
	}

	srcExtent := filepath.Join(src, mmapNodeExtentName(0))
	unusedOffset := mmapNodeTestPageOffset(10, 123)
	flipFileByte(t, srcExtent, unusedOffset)

	dstExtent := filepath.Join(dst, mmapNodeExtentName(0))
	if err := copyMmapNodeExtentFileSparse(srcExtent, dstExtent); err != nil {
		t.Fatal(err)
	}
	got := readFileBytes(t, dstExtent, mmapNodeTestPageOffset(id, 321), len("used page marker"))
	if string(got) != "used page marker" {
		t.Fatalf("copied used page marker = %q, want %q", got, "used page marker")
	}
	unused := readFileBytes(t, dstExtent, unusedOffset, 1)
	if unused[0] != 0 {
		t.Fatalf("copied unused page byte = 0x%x, want 0", unused[0])
	}
}

func TestCopyMmapNodeStoreDirPreservesDestinationDir(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	srcNodes, err := openMmapNodeStore(src)
	if err != nil {
		t.Fatal(err)
	}
	id, page, err := srcNodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	copy(mmapNodePageBytes(page)[321:], []byte("source marker"))
	if err := srcNodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := srcNodes.Close(); err != nil {
		t.Fatal(err)
	}

	dstNodes, err := openMmapNodeStore(dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := dstNodes.Close(); err != nil {
		t.Fatal(err)
	}
	staleExtent, err := createMmapNodeExtent(dst, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := staleExtent.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, mmapNodeExtentName(0)+mmapNodeExtentCopyTempSuffix), []byte("stale temp"), 0o600); err != nil {
		t.Fatal(err)
	}

	before, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyMmapNodeStoreDir(src, dst); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("copyMmapNodeStoreDir replaced destination directory")
	}
	if _, err := os.Stat(filepath.Join(dst, mmapNodeExtentName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale extent stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Join(dst, mmapNodeExtentName(0)+mmapNodeExtentCopyTempSuffix)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp stat err = %v, want not exist", err)
	}
	got := readFileBytes(t, filepath.Join(dst, mmapNodeExtentName(0)), mmapNodeTestPageOffset(id, 321), len("source marker"))
	if string(got) != "source marker" {
		t.Fatalf("copied marker = %q, want source marker", got)
	}
}

func TestCopyMmapNodeStoreDirReplacesExistingExtent(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	srcNodes, err := openMmapNodeStore(src)
	if err != nil {
		t.Fatal(err)
	}
	srcID, srcPage, err := srcNodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	copy(mmapNodePageBytes(srcPage)[321:], []byte("source used page"))
	if err := srcNodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := srcNodes.Close(); err != nil {
		t.Fatal(err)
	}

	dstNodes, err := openMmapNodeStore(dst)
	if err != nil {
		t.Fatal(err)
	}
	dstID, dstPage, err := dstNodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if dstID != srcID {
		t.Fatalf("dst id = %d, want %d", dstID, srcID)
	}
	copy(mmapNodePageBytes(dstPage)[321:], []byte("old used page"))
	if err := dstNodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := dstNodes.Close(); err != nil {
		t.Fatal(err)
	}

	srcExtent := filepath.Join(src, mmapNodeExtentName(0))
	dstExtent := filepath.Join(dst, mmapNodeExtentName(0))
	unusedOffset := mmapNodeTestPageOffset(10, 555)
	writeFileBytes(t, srcExtent, unusedOffset, []byte("src-unused"))
	writeFileBytes(t, dstExtent, unusedOffset, []byte("dst-unused"))

	if err := copyMmapNodeStoreDir(src, dst); err != nil {
		t.Fatal(err)
	}
	gotUsed := readFileBytes(t, dstExtent, mmapNodeTestPageOffset(srcID, 321), len("source used page"))
	if string(gotUsed) != "source used page" {
		t.Fatalf("copied used page = %q, want source used page", gotUsed)
	}
	gotUnused := readFileBytes(t, dstExtent, unusedOffset, len("dst-unused"))
	if string(gotUnused) != "src-unused" {
		t.Fatalf("unused page = %q, want src-unused", gotUnused)
	}
}

func TestMmapNodeStoreReleasesLaterFreeExtents(t *testing.T) {
	dir := t.TempDir()
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if nodes != nil {
			_ = nodes.Close()
		}
	}()

	extent1, err := createMmapNodeExtent(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	extent2, err := createMmapNodeExtent(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	extent3, err := createMmapNodeExtent(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	extent2.setUsed(0, true)
	extent2.setLiveSlots(1)
	extent3.setUsed(0, true)
	extent3.setLiveSlots(1)
	nodes.extents = append(nodes.extents, extent1, extent2, extent3)

	extent3ID := uint64(3 * mmapNodeSlotsPerExtent)
	marker := []byte("later extent stays live")
	copy(mmapNodePageBytes(extent3.page(0))[77:], marker)

	if err := nodes.Free(uint64(2 * mmapNodeSlotsPerExtent)); err != nil {
		t.Fatal(err)
	}
	if nodes.extents[1] == nil {
		t.Fatalf("earlier free extent was released")
	}
	if nodes.extents[2] != nil {
		t.Fatalf("later free extent is still mapped")
	}
	if nodes.extents[3] == nil {
		t.Fatalf("live later extent was released")
	}
	if _, err := os.Stat(filepath.Join(dir, mmapNodeExtentName(2))); !os.IsNotExist(err) {
		t.Fatalf("released extent stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mmapNodeExtentName(1))); err != nil {
		t.Fatalf("kept free extent stat: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mmapNodeExtentName(3))); err != nil {
		t.Fatalf("live extent stat: %v", err)
	}
	if err := nodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		t.Fatal(err)
	}
	nodes = nil

	nodes, err = openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	page, err := nodes.Get(extent3ID)
	if err != nil {
		t.Fatal(err)
	}
	got := mmapNodePageBytes(page)[77 : 77+len(marker)]
	if !bytes.Equal(got, marker) {
		t.Fatalf("live extent marker = %q, want %q", got, marker)
	}
	id, _, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("allocated id = %d, want lowest free id 1", id)
	}
}

func TestMmapNodeStorePersistsIndex(t *testing.T) {
	dir := t.TempDir()
	records := newHeapRecordStore()
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	backend := newIndexBackendWithNodes(records, nodes)
	for i := 0; i < minpatricia.MaxNodeReps+50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if _, err := backend.put([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := backend.delete([]byte("key-0003")); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.put([]byte("key-0004"), []byte("value-key-0004-replaced")); err != nil {
		t.Fatal(err)
	}
	if backend.nodes.LiveNodes() < 2 {
		t.Fatalf("expected multi-node index, live nodes=%d", backend.nodes.LiveNodes())
	}
	if err := nodes.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		t.Fatal(err)
	}

	nodes, err = openMmapNodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, nodes)

	reopened, err := openIndexBackend(records, nodes)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := reopened.get([]byte("key-0004"))
	if err != nil || !ok || string(value) != "value-key-0004-replaced" {
		t.Fatalf("reopened key-0004 = (%q,%v,%v), want replaced value,true,nil", value, ok, err)
	}
	_, ok, err = reopened.get([]byte("key-0003"))
	if err != nil || ok {
		t.Fatalf("reopened key-0003 ok=%v err=%v, want false,nil", ok, err)
	}

	var want []string
	for i := 0; i < minpatricia.MaxNodeReps+50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if key == "key-0003" {
			continue
		}
		value := "value-" + key
		if key == "key-0004" {
			value = "value-key-0004-replaced"
		}
		want = append(want, key+"="+value)
	}
	assertItems(t, "reopened mmap index Scan", reopened.scan, want)
}

func mmapNodeTestPageOffset(id uint64, off int64) int64 {
	slot := id % mmapNodeSlotsPerExtent
	return int64(mmapNodeReservedPages+slot)*mmapNodePageSize + off
}

func flipFileByte(t *testing.T, path string, offset int64) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := file.ReadAt(b[:], offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := file.WriteAt(b[:], offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeFileBytes(t *testing.T, path string, offset int64, data []byte) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(data, offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func readFileBytes(t *testing.T, path string, offset int64, n int) []byte {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	buf := make([]byte, n)
	if _, err := file.ReadAt(buf, offset); err != nil {
		t.Fatal(err)
	}
	return buf
}
