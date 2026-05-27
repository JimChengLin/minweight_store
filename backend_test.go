package minweight_store

import (
	"fmt"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

func TestRecordStoreOwnsRecords(t *testing.T) {
	records := newHeapRecordStore()
	key := []byte("alpha")
	value := []byte("one")
	pos, err := records.Append(key, value)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	value[0] = 'z'

	gotKey, ok := records.Key(pos)
	if !ok || string(gotKey) != "alpha" {
		t.Fatalf("Key(%d) = (%q,%v), want alpha,true", pos, gotKey, ok)
	}
	gotValue, ok := records.Value(pos)
	if !ok || string(gotValue) != "one" {
		t.Fatalf("Value(%d) = (%q,%v), want one,true", pos, gotValue, ok)
	}

	nilKeyPos, err := records.Append(nil, []byte("empty-key"))
	if err != nil {
		t.Fatal(err)
	}
	gotKey, ok = records.Key(nilKeyPos)
	if !ok || gotKey == nil || len(gotKey) != 0 {
		t.Fatalf("Key(%d) = (%v,%v), want non-nil empty key", nilKeyPos, gotKey, ok)
	}

	if err := records.Free(pos); err != nil {
		t.Fatal(err)
	}
	reused, err := records.Append([]byte("bravo"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if reused != pos {
		t.Fatalf("reused position = %d, want %d", reused, pos)
	}
}

func TestHeapNodeStoreAllocFree(t *testing.T) {
	nodes := newHeapNodeStore()
	id, _, err := nodes.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if id == nodes.Root() {
		t.Fatalf("Alloc reused root id")
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

func TestIndexBackendUsesExplicitStores(t *testing.T) {
	backend := newIndexBackend()
	for i := 0; i < minpatricia.MaxNodeReps+50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if err := backend.put([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	if backend.records.Len() != backend.len() {
		t.Fatalf("heapRecord len = %d, index len = %d", backend.records.Len(), backend.len())
	}
	if backend.nodes.LiveNodes() < 2 {
		t.Fatalf("expected multi-node index, live nodes=%d", backend.nodes.LiveNodes())
	}

	reopened, err := openIndexBackend(backend.records, backend.nodes)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := reopened.get([]byte("key-0004"))
	if err != nil || !ok || string(value) != "value-key-0004" {
		t.Fatalf("reopened key-0004 = (%q,%v,%v), want value-key-0004,true,nil", value, ok, err)
	}
	if err := backend.put([]byte("key-0004"), []byte("value-key-0004-replaced")); err != nil {
		t.Fatal(err)
	}
	value, ok, err = backend.get([]byte("key-0004"))
	if err != nil || !ok || string(value) != "value-key-0004-replaced" {
		t.Fatalf("backend key-0004 = (%q,%v,%v), want replaced value,true,nil", value, ok, err)
	}
	if _, err := backend.delete([]byte("key-0003")); err != nil {
		t.Fatal(err)
	}
	_, ok, err = backend.get([]byte("key-0003"))
	if err != nil || ok {
		t.Fatalf("backend key-0003 ok=%v err=%v, want false,nil", ok, err)
	}
}
