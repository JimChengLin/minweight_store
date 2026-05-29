package minweight_store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JimChengLin/minpatricia"
	"github.com/parquet-go/parquet-go"
)

const parquetRecordTestFileNo = 7

func TestParquetRecordStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	input := []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("bravo"), Value: []byte("two")},
		{Key: []byte{}, Value: []byte("empty-key")},
	}

	store, positions := buildParquetRecordStoreForTest(t, path, input)
	defer closeForTest(t, store)

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
	reopened, err := openParquetRecordStore(path, parquetRecordTestFileNo)
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, reopened)
	assertParquetRecord(t, reopened, positions[0], "alpha", "one")
	assertParquetRecord(t, reopened, positions[1], "bravo", "two")
	assertParquetRecord(t, reopened, positions[2], "", "empty-key")
}

func TestParquetRecordStoreWritesIncrementally(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, err := createParquetRecordStore(path, parquetRecordTestFileNo, parquet.MaxRowsPerRowGroup(1))
	if err != nil {
		t.Fatal(err)
	}
	storeSynced := false
	defer func() {
		if !storeSynced {
			_ = store.Abort()
		}
	}()

	key := []byte("alpha")
	value := []byte("one")
	first, err := store.Append(key, value)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	value[0] = 'z'

	second, err := store.Append([]byte("bravo"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if store.build != nil {
		t.Fatalf("build state = %v, want nil", store.build)
	}
	storeSynced = true
	defer closeForTest(t, store)

	assertParquetRecord(t, store, first, "alpha", "one")
	assertParquetRecord(t, store, second, "bravo", "two")
}

func TestParquetRecordStoreAppendAfterSyncFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, _ := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
	})
	defer closeForTest(t, store)

	pos, err := store.Append([]byte("bravo"), []byte("two"))
	if pos != 0 || !errors.Is(err, ErrParquet) {
		t.Fatalf("Append after Sync = (%d,%v), want 0,%v", pos, err, ErrParquet)
	}
}

func TestParquetRecordStoreAbortAfterSyncIsNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, _ := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
	})
	defer closeForTest(t, store)

	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestParquetRecordStoreCloseAbortsBuild(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.parquet")
	store, err := createParquetRecordStore(path, parquetRecordTestFileNo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp stat err = %v, want %v", err, os.ErrNotExist)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final stat err = %v, want %v", err, os.ErrNotExist)
	}
}

func TestParquetRecordStorePositionsUseFileNoAndRowIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("bravo"), Value: []byte("two")},
		{Key: []byte("charlie"), Value: []byte("three")},
	}, parquet.MaxRowsPerRowGroup(1))
	defer closeForTest(t, store)

	for i, pos := range positions {
		fileNo, row, ok := parseParquetRecordPosition(pos)
		if !ok {
			t.Fatalf("position %d did not parse", pos)
		}
		if fileNo != parquetRecordTestFileNo || row != uint64(i) {
			t.Fatalf("position %d = fileNo %d row %d, want %d %d", pos, fileNo, row, parquetRecordTestFileNo, i)
		}
	}
	assertParquetRecord(t, store, positions[2], "charlie", "three")
}

func TestParquetRecordStoreUsesByteArrayColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, _ := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
	})
	defer closeForTest(t, store)

	chunks := store.rowGroups[0].ColumnChunks()
	if len(chunks) != parquetRecordColumnCount {
		t.Fatalf("column chunks = %d, want %d", len(chunks), parquetRecordColumnCount)
	}
	if chunks[parquetRecordKeyColumn].Type().Kind() != parquet.ByteArray {
		t.Fatalf("key column type = %s, want %s", chunks[parquetRecordKeyColumn].Type(), parquet.ByteArray)
	}
	if chunks[parquetRecordValueColumn].Type().Kind() != parquet.ByteArray {
		t.Fatalf("value column type = %s, want %s", chunks[parquetRecordValueColumn].Type(), parquet.ByteArray)
	}
}

func TestParquetRecordStoreDefaultPageSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	records := make([]parquetRecord, 512)
	for i := range records {
		value := make([]byte, 128)
		for j := range value {
			value[j] = byte(i + j)
		}
		records[i] = parquetRecord{
			Key:   []byte{byte(i >> 8), byte(i)},
			Value: value,
		}
	}

	store, _ := buildParquetRecordStoreForTest(t, path, records)
	defer closeForTest(t, store)

	offsetIndex, err := store.rowGroups[0].ColumnChunks()[parquetRecordValueColumn].OffsetIndex()
	if err != nil {
		t.Fatal(err)
	}
	if offsetIndex.NumPages() <= 1 {
		t.Fatalf("value pages = %d, want more than 1", offsetIndex.NumPages())
	}
}

func TestParquetRecordStoreReturnedBytesDoNotAliasPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
	})
	defer closeForTest(t, store)

	keyPages := captureParquetReaderPages(&store.keyReaders[0])
	valuePages := captureParquetReaderPages(&store.valueReaders[0])

	key, ok := store.Key(positions[0])
	if !ok {
		t.Fatal("missing key")
	}
	value, ok := store.Value(positions[0])
	if !ok {
		t.Fatal("missing value")
	}

	poisonParquetPage(t, keyPages.page, "xxxxx")
	poisonParquetPage(t, valuePages.page, "zzz")

	if string(key) != "alpha" {
		t.Fatalf("returned key = %q after page mutation, want alpha", key)
	}
	if string(value) != "one" {
		t.Fatalf("returned value = %q after page mutation, want one", value)
	}
}

func TestParquetRecordStoreInvalidPosition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, _ := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("alpha"), Value: []byte("one")},
	})
	defer closeForTest(t, store)

	if key, ok := store.Key(0); ok || key != nil {
		t.Fatalf("Key(0) = (%q,%v), want nil,false", key, ok)
	}
	if value, ok := store.Value(minpatricia.Position(minpatriciaHandleTag)); ok || value != nil {
		t.Fatalf("Value(tagged) = (%q,%v), want nil,false", value, ok)
	}
	if pos, err := makeParquetRecordPosition(parquetRecordTestFileNo, parquetRecordMaxRowIndex+1); err == nil || pos != 0 {
		t.Fatalf("makeParquetRecordPosition overflow = (%d,%v), want 0,error", pos, err)
	}
}

func TestParquetRecordStoreEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions := buildParquetRecordStoreForTest(t, path, nil)
	defer closeForTest(t, store)

	if store.Len() != 0 {
		t.Fatalf("Len = %d, want 0", store.Len())
	}
	if len(positions) != 0 {
		t.Fatalf("positions len = %d, want 0", len(positions))
	}
}

func TestParquetRecordStoreWorksWithMinpatricia(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	store, positions := buildParquetRecordStoreForTest(t, path, []parquetRecord{
		{Key: []byte("delta"), Value: []byte("four")},
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("charlie"), Value: []byte("three")},
	})
	defer closeForTest(t, store)

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

type capturingParquetPages struct {
	parquet.Pages
	page parquet.Page
}

func captureParquetReaderPages(reader *parquetRecordColumnReader) *capturingParquetPages {
	capture := &capturingParquetPages{
		Pages: reader.pages,
	}
	reader.pages = capture
	return capture
}

func (p *capturingParquetPages) ReadPage() (parquet.Page, error) {
	page, err := p.Pages.ReadPage()
	if err != nil {
		return nil, err
	}
	if p.page != nil {
		parquet.Release(p.page)
	}
	p.page = page
	parquet.Retain(page)
	return page, nil
}

func (p *capturingParquetPages) Close() error {
	if p.page != nil {
		parquet.Release(p.page)
		p.page = nil
	}
	return p.Pages.Close()
}

func poisonParquetPage(t *testing.T, page parquet.Page, replacement string) {
	t.Helper()

	if page == nil {
		t.Fatal("page is nil")
	}
	values := page.Data()
	data, offsets := values.ByteArray()
	if len(offsets) < 2 {
		t.Fatalf("page offsets len = %d, want at least 2", len(offsets))
	}
	value := data[offsets[0]:offsets[1]]
	if len(value) != len(replacement) {
		t.Fatalf("page value len = %d, replacement len = %d", len(value), len(replacement))
	}
	copy(value, replacement)
}

func buildParquetRecordStoreForTest(t testing.TB, path string, records []parquetRecord, options ...parquet.WriterOption) (*parquetRecordStore, []minpatricia.Position) {
	t.Helper()

	store, err := createParquetRecordStore(path, parquetRecordTestFileNo, options...)
	if err != nil {
		t.Fatal(err)
	}
	storeSynced := false
	defer func() {
		if !storeSynced {
			_ = store.Abort()
		}
	}()

	positions := make([]minpatricia.Position, 0, len(records))
	for i := range records {
		pos, err := store.Append(records[i].Key, records[i].Value)
		if err != nil {
			t.Fatal(err)
		}
		positions = append(positions, pos)
	}

	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	storeSynced = true
	return store, positions
}
