package minweight_store

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/JimChengLin/minpatricia"
	"github.com/parquet-go/parquet-go"
)

const (
	parquetRecordRowBits     = 32
	parquetRecordRowMask     = 1<<parquetRecordRowBits - 1
	parquetRecordMaxRow      = parquetRecordRowMask
	parquetRecordMaxRowGroup = 1<<31 - 2
)

type parquetRecord struct {
	Key   []byte `parquet:"key"`
	Value []byte `parquet:"value"`
}

type parquetRecordStore struct {
	path            string
	file            *os.File
	records         []parquetRecord
	positions       []minpatricia.Position
	rowGroupOffsets []uint64
}

func writeParquetRecordStore(path string, records []parquetRecord, options ...parquet.WriterOption) (*parquetRecordStore, []minpatricia.Position, error) {
	rows := cloneParquetRecords(records)
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, nil, err
	}

	ok := false
	closed := false
	defer func() {
		if !ok {
			if !closed {
				_ = file.Close()
			}
			_ = os.Remove(tmp)
		}
	}()

	writer := parquet.NewGenericWriter[parquetRecord](file, options...)
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return nil, nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, nil, err
	}
	if err := file.Close(); err != nil {
		return nil, nil, err
	}
	closed = true
	if err := os.Rename(tmp, path); err != nil {
		return nil, nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, nil, err
	}
	ok = true

	store, err := openParquetRecordStore(path)
	if err != nil {
		return nil, nil, err
	}
	return store, clonePositions(store.positions), nil
}

func openParquetRecordStore(path string) (*parquetRecordStore, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return nil, err
	}
	records, err := readParquetRecords(parquetFile)
	if err != nil {
		return nil, err
	}
	offsets, positions, err := buildParquetRecordLayout(parquetFile.RowGroups())
	if err != nil {
		return nil, err
	}
	if len(records) != len(positions) {
		return nil, ErrParquet
	}

	ok = true
	return &parquetRecordStore{
		path:            path,
		file:            file,
		records:         records,
		positions:       positions,
		rowGroupOffsets: offsets,
	}, nil
}

func (s *parquetRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	index, ok := s.rowIndex(pos)
	if !ok {
		return nil, false
	}
	return s.records[index].Key, true
}

func (s *parquetRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	index, ok := s.rowIndex(pos)
	if !ok {
		return nil, false
	}
	return s.records[index].Value, true
}

func (s *parquetRecordStore) Len() int {
	return len(s.records)
}

func (s *parquetRecordStore) Positions() []minpatricia.Position {
	return clonePositions(s.positions)
}

func (s *parquetRecordStore) Sync() error {
	return nil
}

func (s *parquetRecordStore) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *parquetRecordStore) rowIndex(pos minpatricia.Position) (int, bool) {
	rowGroup, row, ok := parseParquetRecordPosition(pos)
	if !ok || rowGroup >= uint64(len(s.rowGroupOffsets)-1) {
		return 0, false
	}
	start := s.rowGroupOffsets[rowGroup]
	end := s.rowGroupOffsets[rowGroup+1]
	if row >= end-start {
		return 0, false
	}
	index := start + row
	if index > uint64(maxInt) {
		return 0, false
	}
	return int(index), true
}

func readParquetRecords(file *parquet.File) ([]parquetRecord, error) {
	if file.NumRows() > int64(maxInt) {
		return nil, ErrParquet
	}
	reader := parquet.NewGenericReader[parquetRecord](file)
	defer reader.Close()

	records := make([]parquetRecord, int(file.NumRows()))
	n, err := reader.Read(records)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n != len(records) {
		return nil, ErrParquet
	}
	for i := range records {
		records[i].Key = cloneRecordKey(records[i].Key)
		records[i].Value = cloneBytes(records[i].Value)
	}
	return records, nil
}

func buildParquetRecordLayout(rowGroups []parquet.RowGroup) ([]uint64, []minpatricia.Position, error) {
	offsets := make([]uint64, len(rowGroups)+1)
	var positions []minpatricia.Position
	var total uint64
	for rowGroup, group := range rowGroups {
		if rowGroup > parquetRecordMaxRowGroup {
			return nil, nil, ErrParquet
		}
		rows := group.NumRows()
		if rows < 0 || uint64(rows) > parquetRecordMaxRow+1 {
			return nil, nil, ErrParquet
		}
		offsets[rowGroup] = total
		for row := uint64(0); row < uint64(rows); row++ {
			pos, err := makeParquetRecordPosition(uint64(rowGroup), row)
			if err != nil {
				return nil, nil, err
			}
			positions = append(positions, pos)
		}
		total += uint64(rows)
	}
	offsets[len(rowGroups)] = total
	return offsets, positions, nil
}

func makeParquetRecordPosition(rowGroup, row uint64) (minpatricia.Position, error) {
	if rowGroup > parquetRecordMaxRowGroup || row > parquetRecordMaxRow {
		return 0, ErrParquet
	}
	return minpatricia.Position(((rowGroup + 1) << parquetRecordRowBits) | row), nil
}

func parseParquetRecordPosition(pos minpatricia.Position) (uint64, uint64, bool) {
	raw := uint64(pos)
	if raw == 0 || raw&minpatriciaHandleTag != 0 {
		return 0, 0, false
	}
	group := raw >> parquetRecordRowBits
	if group == 0 {
		return 0, 0, false
	}
	return group - 1, raw & parquetRecordRowMask, true
}

func cloneParquetRecords(records []parquetRecord) []parquetRecord {
	cloned := make([]parquetRecord, len(records))
	for i := range records {
		cloned[i] = parquetRecord{
			Key:   cloneRecordKey(records[i].Key),
			Value: cloneBytes(records[i].Value),
		}
	}
	return cloned
}

func clonePositions(positions []minpatricia.Position) []minpatricia.Position {
	return append([]minpatricia.Position(nil), positions...)
}
