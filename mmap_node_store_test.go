//go:build darwin || linux

package minweight_store

import (
	"bytes"
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
	defer nodes.Close()

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
	defer nodes.Close()

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
	defer nodes.Close()

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
	defer nodes.Close()

	extent := nodes.extents[0]
	bitmap := extent.bitmap()
	for byteIndex := 0; byteIndex < mmapNodeBitmapBytes; byteIndex++ {
		bitmap[byteIndex] = mmapNodeBitmapByteMask(byteIndex)
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
	defer nodes.Close()

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
		if err := backend.put([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := backend.delete([]byte("key-0003")); err != nil {
		t.Fatal(err)
	}
	if err := backend.put([]byte("key-0004"), []byte("value-key-0004-replaced")); err != nil {
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
	defer nodes.Close()

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
