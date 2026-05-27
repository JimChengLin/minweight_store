package minweight_store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/JimChengLin/minpatricia"
	"github.com/parquet-go/parquet-go"
)

func TestParquetRecordStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	input := []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("bravo"), Value: []byte("two")},
		{Key: []byte{}, Value: []byte("empty-key")},
	}

	store, positions, err := writeParquetRecordStore(path, input)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	input[0].Key[0] = 'x'
	input[0].Value[0] = 'z'

	if store.Len() != len(input) {
		t.Fatalf("Len = %d, want %d", store.Len(), len(input))
	}
	if len(positions) != len(input) {
		t.Fatalf("positions len = %d, want %d", len(positions), len(input))
	}
	assertParquetRecord(t, store, positions[0], "alpha", "one")
	assertParquetRecord(t, store, positions[1], "bravo", "two")
	assertParquetRecord(t, store, positions[2], "", "empty-key")

	if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data[:4], []byte("PAR1")) {
		t.Fatalf("parquet header = %q err=%v, want PAR1,nil", data[:4], err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openParquetRecordStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertParquetRecord(t, store, positions[0], "alpha", "one")
	assertParquetRecord(t, store, positions[1], "bravo", "two")
	assertParquetRecord(t, store, positions[2], "", "empty-key")
}

func TestParquetRecordStorePositionsUseRowGroupAndRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions, err := writeParquetRecordStore(path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("bravo"), Value: []byte("two")},
		{Key: []byte("charlie"), Value: []byte("three")},
	}, parquet.MaxRowsPerRowGroup(1))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for i, pos := range positions {
		rowGroup, row, ok := parseParquetRecordPosition(pos)
		if !ok {
			t.Fatalf("position %d did not parse", pos)
		}
		if rowGroup != uint64(i) || row != 0 {
			t.Fatalf("position %d = rowGroup %d row %d, want %d 0", pos, rowGroup, row, i)
		}
	}
	assertParquetRecord(t, store, positions[2], "charlie", "three")
}

func TestParquetRecordStoreInvalidPosition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, _, err := writeParquetRecordStore(path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if key, ok := store.Key(0); ok || key != nil {
		t.Fatalf("Key(0) = (%q,%v), want nil,false", key, ok)
	}
	if value, ok := store.Value(minpatricia.Position(minpatriciaHandleTag)); ok || value != nil {
		t.Fatalf("Value(tagged) = (%q,%v), want nil,false", value, ok)
	}
	if pos, err := makeParquetRecordPosition(parquetRecordMaxRowGroup+1, 0); err == nil || pos != 0 {
		t.Fatalf("makeParquetRecordPosition overflow = (%d,%v), want 0,error", pos, err)
	}
}

func TestParquetRecordStoreEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions, err := writeParquetRecordStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if store.Len() != 0 {
		t.Fatalf("Len = %d, want 0", store.Len())
	}
	if len(positions) != 0 {
		t.Fatalf("positions len = %d, want 0", len(positions))
	}
}

func TestParquetRecordStoreWorksWithMinpatricia(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions, err := writeParquetRecordStore(path, []parquetRecord{
		{Key: []byte("delta"), Value: []byte("four")},
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("charlie"), Value: []byte("three")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	index := minpatricia.NewWithRecords(store)
	for i, key := range [][]byte{[]byte("delta"), []byte("alpha"), []byte("charlie")} {
		if _, _, err := index.Put(key, positions[i]); err != nil {
			t.Fatal(err)
		}
	}

	pos, ok, err := index.Get([]byte("charlie"))
	if err != nil || !ok {
		t.Fatalf("Get(charlie) = (%d,%v,%v), want position,true,nil", pos, ok, err)
	}
	value, ok := store.Value(pos)
	if !ok || string(value) != "three" {
		t.Fatalf("Value(charlie position) = (%q,%v), want three,true", value, ok)
	}
}

func assertParquetRecord(t *testing.T, store *parquetRecordStore, pos minpatricia.Position, key, value string) {
	t.Helper()

	gotKey, ok := store.Key(pos)
	if !ok || string(gotKey) != key {
		t.Fatalf("Key(%d) = (%q,%v), want %q,true", pos, gotKey, ok, key)
	}
	gotValue, ok := store.Value(pos)
	if !ok || string(gotValue) != value {
		t.Fatalf("Value(%d) = (%q,%v), want %q,true", pos, gotValue, ok, value)
	}
}
