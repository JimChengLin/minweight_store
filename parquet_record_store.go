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
	parquetRecordRowIndexBits       = 32
	parquetRecordRowIndexMask       = 1<<parquetRecordRowIndexBits - 1
	parquetRecordMaxRowIndex        = parquetRecordRowIndexMask
	parquetRecordMaxRowsPerRowGroup = parquetRecordMaxRowIndex + 1
	parquetRecordMaxEncodedRowGroup = minpatriciaHandleTag>>parquetRecordRowIndexBits - 1
	parquetRecordMaxRowGroupIndex   = parquetRecordMaxEncodedRowGroup - 1
)

type parquetRecord struct {
	Key   []byte `parquet:"key"`
	Value []byte `parquet:"value"`
}

type parquetRecordKey struct {
	Key []byte `parquet:"key"`
}

type parquetRecordValue struct {
	Value []byte `parquet:"value"`
}

type parquetRecordStore struct {
	path      string
	file      *os.File
	rowGroups []parquet.RowGroup
	build     *parquetRecordStoreBuilder
}

type parquetRecordStoreBuilder struct {
	tmpPath            string
	writer             *parquet.GenericWriter[parquetRecord]
	file               *os.File
	maxRowsPerRowGroup uint64
	rowGroupIndex      uint64
	rowIndex           uint64
}

func createParquetRecordStore(path string, options ...parquet.WriterOption) (*parquetRecordStore, error) {
	config, err := parquet.NewWriterConfig(options...)
	if err != nil {
		return nil, err
	}
	if config.MaxRowsPerRowGroup == parquet.DefaultMaxRowsPerRowGroup {
		config.MaxRowsPerRowGroup = parquetRecordMaxRowsPerRowGroup
	}
	if config.MaxRowsPerRowGroup <= 0 || uint64(config.MaxRowsPerRowGroup) > parquetRecordMaxRowsPerRowGroup {
		return nil, ErrParquet
	}

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}

	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(tmp)
		}
	}()

	store := &parquetRecordStore{
		path: path,
		build: &parquetRecordStoreBuilder{
			tmpPath:            tmp,
			file:               file,
			writer:             parquet.NewGenericWriter[parquetRecord](file, config),
			maxRowsPerRowGroup: uint64(config.MaxRowsPerRowGroup),
		},
	}
	ok = true
	return store, nil
}

func (s *parquetRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	build := s.build
	pos, err := makeParquetRecordPosition(build.rowGroupIndex, build.rowIndex)
	if err != nil {
		return 0, err
	}

	written, err := build.writer.Write([]parquetRecord{{
		Key:   key,
		Value: value,
	}})
	if err != nil {
		return 0, err
	}
	if written != 1 {
		return 0, ErrParquet
	}

	build.rowIndex++
	if build.rowIndex == build.maxRowsPerRowGroup {
		build.rowIndex = 0
		build.rowGroupIndex++
	}
	return pos, nil
}

func (s *parquetRecordStore) Sync() error {
	if s.build == nil {
		return nil
	}

	build := s.build
	if err := build.writer.Close(); err != nil {
		return err
	}
	build.writer = nil
	if err := build.file.Sync(); err != nil {
		return err
	}
	if err := build.file.Close(); err != nil {
		return err
	}
	build.file = nil
	if err := os.Rename(build.tmpPath, s.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return err
	}

	store, err := openParquetRecordStore(s.path)
	if err != nil {
		return err
	}
	// Replace the writable store with the read-only view; this drops the builder.
	*s = *store
	return nil
}

func (s *parquetRecordStore) Abort() error {
	build := s.build
	var firstErr error
	if build.writer != nil {
		if err := build.writer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		build.writer = nil
	}
	if build.file != nil {
		if err := build.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		build.file = nil
	}
	if err := os.Remove(build.tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
		firstErr = err
	}
	s.build = nil
	return firstErr
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
	rowGroups := parquetFile.RowGroups()
	if err := validateParquetRecordLayout(rowGroups, parquetFile.NumRows()); err != nil {
		return nil, err
	}

	ok = true
	return &parquetRecordStore{
		path:      path,
		file:      file,
		rowGroups: rowGroups,
	}, nil
}

func (s *parquetRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	record, ok := readParquetRecordProjection[parquetRecordKey](s, pos)
	if !ok {
		return nil, false
	}
	return record.Key, true
}

func (s *parquetRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	record, ok := readParquetRecordProjection[parquetRecordValue](s, pos)
	if !ok {
		return nil, false
	}
	return record.Value, true
}

func (s *parquetRecordStore) Len() int {
	var rows int
	for _, group := range s.rowGroups {
		rows += int(group.NumRows())
	}
	return rows
}

func (s *parquetRecordStore) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func readParquetRecordProjection[T any](s *parquetRecordStore, pos minpatricia.Position) (T, bool) {
	var zero T
	rowGroup, row, ok := parseParquetRecordPosition(pos)
	if !ok || rowGroup >= uint64(len(s.rowGroups)) {
		return zero, false
	}

	group := s.rowGroups[rowGroup]
	if row >= uint64(group.NumRows()) {
		return zero, false
	}
	reader := parquet.NewGenericRowGroupReader[T](group)
	defer reader.Close()
	if err := reader.SeekToRow(int64(row)); err != nil {
		return zero, false
	}
	records := [1]T{}
	n, err := reader.Read(records[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return zero, false
	}
	if n != 1 {
		return zero, false
	}
	return records[0], true
}

func validateParquetRecordLayout(rowGroups []parquet.RowGroup, rows int64) error {
	if rows < 0 || rows > int64(maxInt) {
		return ErrParquet
	}
	var total uint64
	for rowGroup, group := range rowGroups {
		if uint64(rowGroup) > parquetRecordMaxRowGroupIndex {
			return ErrParquet
		}
		groupRows := group.NumRows()
		if groupRows < 0 || uint64(groupRows) > parquetRecordMaxRowsPerRowGroup {
			return ErrParquet
		}
		total += uint64(groupRows)
		if total > uint64(maxInt) {
			return ErrParquet
		}
	}
	if total != uint64(rows) {
		return ErrParquet
	}
	return nil
}

func makeParquetRecordPosition(rowGroup, row uint64) (minpatricia.Position, error) {
	if rowGroup > parquetRecordMaxRowGroupIndex || row > parquetRecordMaxRowIndex {
		return 0, ErrParquet
	}
	return minpatricia.Position(((rowGroup + 1) << parquetRecordRowIndexBits) | row), nil
}

func parseParquetRecordPosition(pos minpatricia.Position) (uint64, uint64, bool) {
	raw := uint64(pos)
	if raw == 0 || raw&minpatriciaHandleTag != 0 {
		return 0, 0, false
	}
	group := raw >> parquetRecordRowIndexBits
	if group == 0 {
		return 0, 0, false
	}
	return group - 1, raw & parquetRecordRowIndexMask, true
}
