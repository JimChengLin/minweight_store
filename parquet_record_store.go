package minweight_store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/JimChengLin/minpatricia"
	"github.com/parquet-go/parquet-go"
)

const (
	parquetRecordMaxRowIndex        = recordOffsetMask
	parquetRecordMaxRowsPerRowGroup = int64(recordOffsetLimit)
	parquetRecordKeyColumn          = 0
	parquetRecordValueColumn        = 1
	parquetRecordColumnCount        = 2
	parquetRecordDefaultPageSize    = 4 << 10
)

type parquetRecord struct {
	Key   []byte `parquet:"key"`
	Value []byte `parquet:"value"`
}

type parquetRecordKey struct {
	Key []byte `parquet:"key"`
}

type parquetRecordStore struct {
	path         string
	fileNo       uint64
	file         *os.File
	rowGroups    []parquet.RowGroup
	rowStarts    []uint64
	keyReaders   []parquetRecordColumnReader
	valueReaders []parquetRecordColumnReader
	build        *parquetRecordStoreBuilder
}

type parquetRecordStoreBuilder struct {
	tmpPath            string
	writer             *parquet.GenericWriter[parquetRecord]
	file               *os.File
	maxRowsPerRowGroup uint64
	rowIndex           uint64
}

type parquetRecordColumnReader struct {
	mu    sync.Mutex
	pages parquet.Pages
}

func createParquetRecordStore(path string, fileNo uint64, options ...parquet.WriterOption) (*parquetRecordStore, error) {
	if fileNo == 0 || fileNo >= recordFileNoLimit {
		return nil, minpatricia.ErrPositionTag
	}
	config, err := parquet.NewWriterConfig(options...)
	if err != nil {
		return nil, err
	}
	if config.MaxRowsPerRowGroup == parquet.DefaultMaxRowsPerRowGroup {
		config.MaxRowsPerRowGroup = parquetRecordMaxRowsPerRowGroup
	}
	if config.PageBufferSize == parquet.DefaultPageBufferSize {
		config.PageBufferSize = parquetRecordDefaultPageSize
	}
	if config.MaxRowsPerRowGroup <= 0 || config.MaxRowsPerRowGroup > parquetRecordMaxRowsPerRowGroup {
		return nil, ErrParquet
	}

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}

	storeOwnedByCaller := false
	defer func() {
		if !storeOwnedByCaller {
			_ = file.Close()
			_ = os.Remove(tmp)
		}
	}()

	store := &parquetRecordStore{
		path:   path,
		fileNo: fileNo,
		build: &parquetRecordStoreBuilder{
			tmpPath:            tmp,
			file:               file,
			writer:             parquet.NewGenericWriter[parquetRecord](file, config),
			maxRowsPerRowGroup: uint64(config.MaxRowsPerRowGroup),
		},
	}
	storeOwnedByCaller = true
	return store, nil
}

func (s *parquetRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	build := s.build
	if build == nil {
		return 0, ErrParquet
	}
	pos, err := makeParquetRecordPosition(s.fileNo, build.rowIndex)
	if err != nil {
		return 0, err
	}

	records := [1]parquetRecord{{
		Key:   key,
		Value: value,
	}}
	written, err := build.writer.Write(records[:])
	if err != nil {
		return 0, err
	}
	if written != 1 {
		return 0, ErrParquet
	}

	build.rowIndex++
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

	store, err := openParquetRecordStore(s.path, s.fileNo)
	if err != nil {
		return err
	}
	// Replace the writable store with the read-only view; this drops the builder.
	*s = *store
	return nil
}

func (s *parquetRecordStore) Abort() error {
	build := s.build
	if build == nil {
		return nil
	}
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

func openParquetRecordStore(path string, fileNo uint64) (*parquetRecordStore, error) {
	if fileNo == 0 || fileNo >= recordFileNoLimit {
		return nil, minpatricia.ErrPositionTag
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	storeOwnedByCaller := false
	defer func() {
		if !storeOwnedByCaller {
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

	storeOwnedByCaller = true
	return &parquetRecordStore{
		path:         path,
		fileNo:       fileNo,
		file:         file,
		rowGroups:    rowGroups,
		rowStarts:    parquetRecordRowStarts(rowGroups),
		keyReaders:   newParquetRecordColumnReaders(rowGroups, parquetRecordKeyColumn),
		valueReaders: newParquetRecordColumnReaders(rowGroups, parquetRecordValueColumn),
	}, nil
}

func (s *parquetRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	rowGroup, row, ok := s.recordLocation(pos)
	if !ok {
		return nil, false
	}
	key, ok := s.keyReaders[rowGroup].read(row)
	if !ok {
		return nil, false
	}
	return key, true
}

func (s *parquetRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	rowGroup, row, ok := s.recordLocation(pos)
	if !ok {
		return nil, false
	}
	value, ok := s.valueReaders[rowGroup].read(row)
	if !ok {
		return nil, false
	}
	return value, true
}

func (s *parquetRecordStore) Len() int {
	var rows int
	for _, group := range s.rowGroups {
		rows += int(group.NumRows())
	}
	return rows
}

func (s *parquetRecordStore) Close() error {
	var firstErr error
	if s.build != nil {
		if err := s.Abort(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for i := range s.keyReaders {
		if err := s.keyReaders[i].close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.keyReaders = nil
	for i := range s.valueReaders {
		if err := s.valueReaders[i].close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.valueReaders = nil
	if s.file == nil {
		return firstErr
	}
	if err := s.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.file = nil
	return firstErr
}

func (s *parquetRecordStore) closeAfterSync() error {
	return s.Close()
}

func (s *parquetRecordStore) scanKeys(fn func(rowIndex uint64, key []byte) error) error {
	reader := s.newKeyReader()
	defer func() {
		_ = reader.close()
	}()
	for {
		rowIndex, key, ok, err := reader.next()
		if err != nil || !ok {
			return err
		}
		if err := fn(rowIndex, key); err != nil {
			return err
		}
	}
}

type parquetRecordKeyReader struct {
	store       *parquetRecordStore
	nextGroup   int
	rowIndex    uint64
	reader      *parquet.GenericReader[parquetRecordKey]
	buffer      []parquetRecordKey
	bufferIndex int
	bufferLen   int
	pendingErr  error
}

func (s *parquetRecordStore) newKeyReader() *parquetRecordKeyReader {
	return &parquetRecordKeyReader{
		store:  s,
		buffer: make([]parquetRecordKey, 256),
	}
}

func (r *parquetRecordKeyReader) next() (uint64, []byte, bool, error) {
	for {
		if r.bufferIndex < r.bufferLen {
			record := r.buffer[r.bufferIndex]
			rowIndex := r.rowIndex
			r.bufferIndex++
			r.rowIndex++
			return rowIndex, record.Key, true, nil
		}
		r.bufferIndex = 0
		r.bufferLen = 0
		if r.pendingErr != nil {
			err := r.pendingErr
			r.pendingErr = nil
			if err := r.close(); err != nil {
				return 0, nil, false, err
			}
			if errors.Is(err, io.EOF) {
				continue
			}
			return 0, nil, false, err
		}
		if r.reader == nil {
			if r.nextGroup >= len(r.store.rowGroups) {
				return 0, nil, false, nil
			}
			r.reader = parquet.NewGenericRowGroupReader[parquetRecordKey](r.store.rowGroups[r.nextGroup])
			r.rowIndex = r.store.rowStarts[r.nextGroup]
			r.nextGroup++
		}
		n, err := r.reader.Read(r.buffer)
		r.bufferLen = n
		if err != nil {
			r.pendingErr = err
		}
	}
}

func (r *parquetRecordKeyReader) close() error {
	if r.reader == nil {
		return nil
	}
	err := r.reader.Close()
	r.reader = nil
	return err
}

func (s *parquetRecordStore) recordLocation(pos minpatricia.Position) (int, int64, bool) {
	fileNo, rowIndex, ok := parseParquetRecordPosition(pos)
	if !ok || fileNo != s.fileNo {
		return 0, 0, false
	}
	rowGroup := sort.Search(len(s.rowStarts), func(i int) bool {
		return s.rowStarts[i] > rowIndex
	}) - 1
	if rowGroup < 0 {
		return 0, 0, false
	}
	start := s.rowStarts[rowGroup]
	rows := uint64(s.rowGroups[rowGroup].NumRows())
	if rowIndex >= start+rows {
		return 0, 0, false
	}
	return rowGroup, int64(rowIndex - start), true
}

func parquetRecordRowStarts(rowGroups []parquet.RowGroup) []uint64 {
	starts := make([]uint64, len(rowGroups))
	var next uint64
	for i, group := range rowGroups {
		starts[i] = next
		next += uint64(group.NumRows())
	}
	return starts
}

func newParquetRecordColumnReaders(rowGroups []parquet.RowGroup, column int) []parquetRecordColumnReader {
	readers := make([]parquetRecordColumnReader, len(rowGroups))
	for i, group := range rowGroups {
		readers[i].pages = group.ColumnChunks()[column].Pages()
	}
	return readers
}

func (r *parquetRecordColumnReader) read(row int64) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pages == nil {
		return nil, false
	}
	if err := r.pages.SeekToRow(row); err != nil {
		return nil, false
	}
	page, err := r.pages.ReadPage()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	defer parquet.Release(page)

	value, ok := firstParquetByteArray(page)
	if ok {
		return cloneBytes(value), true
	}

	values := [1]parquet.Value{}
	n, err := page.Values().ReadValues(values[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false
	}
	if n != 1 {
		return nil, false
	}
	return cloneBytes(values[0].ByteArray()), true
}

func firstParquetByteArray(page parquet.Page) ([]byte, bool) {
	if page.Dictionary() != nil {
		return nil, false
	}
	values := page.Data()
	data, offsets := values.ByteArray()
	if len(offsets) < 2 {
		return nil, false
	}
	return data[offsets[0]:offsets[1]:offsets[1]], true
}

func (r *parquetRecordColumnReader) close() error {
	if r.pages == nil {
		return nil
	}
	err := r.pages.Close()
	r.pages = nil
	return err
}

func validateParquetRecordLayout(rowGroups []parquet.RowGroup, rows int64) error {
	if rows < 0 || uint64(rows) > recordOffsetLimit {
		return ErrParquet
	}
	var total uint64
	for _, group := range rowGroups {
		chunks := group.ColumnChunks()
		if len(chunks) != parquetRecordColumnCount ||
			chunks[parquetRecordKeyColumn].Type().Kind() != parquet.ByteArray ||
			chunks[parquetRecordValueColumn].Type().Kind() != parquet.ByteArray {
			return ErrParquet
		}
		groupRows := group.NumRows()
		if groupRows < 0 || groupRows > parquetRecordMaxRowsPerRowGroup {
			return ErrParquet
		}
		total += uint64(groupRows)
		if total > recordOffsetLimit {
			return ErrParquet
		}
	}
	if total != uint64(rows) {
		return ErrParquet
	}
	return nil
}

func makeParquetRecordPosition(fileNo, row uint64) (minpatricia.Position, error) {
	if row > parquetRecordMaxRowIndex {
		return 0, ErrParquet
	}
	return makeRecordPosition(fileNo, row)
}

func parseParquetRecordPosition(pos minpatricia.Position) (uint64, uint64, bool) {
	if uint64(pos)&minpatriciaHandleTag != 0 {
		return 0, 0, false
	}
	fileNo := recordPositionFileNo(pos)
	if fileNo == 0 {
		return 0, 0, false
	}
	return fileNo, recordPositionOffset(pos), true
}
