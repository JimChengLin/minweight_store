//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"syscall"

	"github.com/JimChengLin/minpatricia"
)

const (
	walVersion uint32 = 1

	walHeaderVersionOffset = 8
	walHeaderUsedOffset    = 16

	walRecordHeaderSize  = 13
	walRecordOpOffset    = 0
	walRecordKeyOffset   = 1
	walRecordValueOffset = 5
	walRecordCRCOffset   = 9

	walOpPut             = 1
	walOpDelete          = 2
	walOpInstallSST      = 3
	walOpInstallSSTBatch = 4
	walOpWriteBatch      = 5

	walBatchEntryPut    = 0x81
	walBatchEntryDelete = 0x82

	walInstallSSTPayloadSize     = 16
	walInstallSSTBatchHeaderSize = 8
)

var walHeaderMagic = [8]byte{'M', 'W', 'W', 'A', 'L', '0', '1', 0}

// WALReplayPolicy controls how WAL replay handles corrupt records.
type WALReplayPolicy uint8

const (
	// WALReplayPointInTime replays the valid prefix and truncates the WAL there.
	WALReplayPointInTime WALReplayPolicy = iota
	// WALReplayStrict fails Open on the first corrupt WAL record.
	WALReplayStrict
	// WALReplayBestEffort deletes corrupt bytes before replaying CRC-valid records.
	WALReplayBestEffort
)

type mmapWALRecordStore struct {
	fileNo        uint64
	file          *os.File
	data          []byte
	size          uint64
	used          uint64
	sealed        bool
	dataDirty     bool
	metadataDirty bool
}

func openMmapWALRecordStore(path string, size int64, fileNo uint64) (*mmapWALRecordStore, error) {
	if size < walHeaderSize+walRecordHeaderSize {
		return nil, ErrWalFull
	}
	if uint64(size) > recordOffsetLimit {
		return nil, minpatricia.ErrPositionTag
	}
	// Fail fast if fileNo cannot be encoded into a record position.
	if _, err := makeRecordPosition(fileNo, walHeaderSize); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	fileOwnedByStore := false
	defer func() {
		if !fileOwnedByStore {
			_ = file.Close()
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	metadataDirty := false
	if info.Size() == 0 {
		if err := file.Truncate(size); err != nil {
			return nil, err
		}
		metadataDirty = true
	} else if info.Size() != size {
		return nil, errors.Join(ErrCorruptWAL, errors.New("wal size does not match configured size"))
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	store := &mmapWALRecordStore{
		fileNo:        fileNo,
		file:          file,
		data:          data,
		size:          uint64(size),
		metadataDirty: metadataDirty,
	}
	if isZeroBytes(data[:walHeaderSize]) {
		store.initHeader()
	} else if err := store.loadHeader(); err != nil {
		_ = syscall.Munmap(data)
		return nil, err
	}
	fileOwnedByStore = true
	return store, nil
}

func (s *mmapWALRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	if len(key) > minpatricia.MaxKeySize {
		return 0, minpatricia.ErrKeyTooLarge
	}
	return s.appendRecord(walOpPut, key, value)
}

func (s *mmapWALRecordStore) Delete(key []byte) (minpatricia.Position, error) {
	if len(key) > minpatricia.MaxKeySize {
		return 0, minpatricia.ErrKeyTooLarge
	}
	return s.appendRecord(walOpDelete, key, nil)
}

func (s *mmapWALRecordStore) AppendInstallSSTRecord(sourceWALFileNo, sstFileNo uint64) (minpatricia.Position, error) {
	var payload [walInstallSSTPayloadSize]byte
	binary.LittleEndian.PutUint64(payload[:8], sourceWALFileNo)
	binary.LittleEndian.PutUint64(payload[8:], sstFileNo)
	return s.appendRecord(walOpInstallSST, payload[:], nil)
}

func (s *mmapWALRecordStore) AppendInstallSSTBatchRecord(oldSSTFileNos, newSSTFileNos []uint64) (minpatricia.Position, error) {
	payload, err := encodeInstallSSTBatchPayload(oldSSTFileNos, newSSTFileNos)
	if err != nil {
		return 0, err
	}
	return s.appendRecord(walOpInstallSSTBatch, payload, nil)
}

func (s *mmapWALRecordStore) AppendWriteBatch(ops []writeBatchOperation) ([]writeBatchRecord, error) {
	limit := min(s.size, recordOffsetLimit)
	if s.used+walRecordHeaderSize > limit {
		return nil, ErrWalFull
	}

	maxPayload := limit - s.used - walRecordHeaderSize
	payload, relativeOffsets, err := encodeWriteBatchPayload(ops, maxPayload)
	if err != nil {
		return nil, err
	}
	batchPos, err := s.appendRecord(walOpWriteBatch, payload, nil)
	if err != nil {
		return nil, err
	}

	batchPayloadOffset := recordPositionOffset(batchPos) + walRecordHeaderSize
	records := make([]writeBatchRecord, len(ops))
	for i, op := range ops {
		pos, err := makeRecordPosition(s.fileNo, batchPayloadOffset+relativeOffsets[i])
		if err != nil {
			return nil, err
		}
		records[i] = writeBatchRecord{
			op:  op.op,
			key: op.key,
			pos: pos,
		}
	}
	return records, nil
}

func (s *mmapWALRecordStore) Free(pos minpatricia.Position) error {
	return nil
}

func (s *mmapWALRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	rec, err := s.recordAt(pos, false)
	if err != nil {
		return nil, false
	}
	return rec.key, true
}

func (s *mmapWALRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	rec, err := s.recordAt(pos, false)
	if err != nil || rec.op != walOpPut {
		return nil, false
	}
	return rec.value, true
}

func (s *mmapWALRecordStore) OwnedValue(pos minpatricia.Position) ([]byte, bool) {
	value, ok := s.Value(pos)
	if !ok {
		return nil, false
	}
	return cloneBytes(value), true
}

func (s *mmapWALRecordStore) Len() int {
	return 0
}

func (s *mmapWALRecordStore) Replay(policy WALReplayPolicy, fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	switch policy {
	case WALReplayStrict:
		return s.replayStrict(fn)
	case WALReplayPointInTime:
		return s.replayPointInTime(fn)
	case WALReplayBestEffort:
		if err := s.repairBestEffort(); err != nil {
			return err
		}
		return s.replayStrict(fn)
	default:
		return ErrReplayPolicy
	}
}

func (s *mmapWALRecordStore) replayStrict(fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	offset := uint64(walHeaderSize)
	for offset < s.used {
		rec, err := s.recordAtOffset(offset, true)
		if err != nil {
			return err
		}
		next, err := s.replayDecodedRecord(offset, rec, fn)
		if err != nil {
			return err
		}
		offset = next
	}
	if offset != s.used {
		return ErrCorruptWAL
	}
	return nil
}

func (s *mmapWALRecordStore) replayPointInTime(fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	offset := uint64(walHeaderSize)
	lastGoodOffset := offset
	for offset < s.used {
		rec, err := s.recordAtOffset(offset, true)
		if err != nil {
			return s.truncate(lastGoodOffset)
		}
		next, err := s.replayDecodedRecord(offset, rec, fn)
		if err != nil {
			return err
		}
		offset = next
		lastGoodOffset = offset
	}
	return nil
}

func (s *mmapWALRecordStore) replayDecodedRecord(offset uint64, rec walRecord, fn func(op byte, key []byte, pos minpatricia.Position) error) (uint64, error) {
	if rec.op != walOpWriteBatch {
		pos, err := makeRecordPosition(s.fileNo, offset)
		if err != nil {
			return 0, err
		}
		return rec.end, fn(rec.op, rec.key, pos)
	}

	payloadOffset := offset + walRecordHeaderSize
	err := forEachWriteBatchPayloadEntry(rec.key, func(op byte, key, value []byte, relativeOffset uint64) error {
		pos, err := makeRecordPosition(s.fileNo, payloadOffset+relativeOffset)
		if err != nil {
			return err
		}
		return fn(op, key, pos)
	})
	return rec.end, err
}

func (s *mmapWALRecordStore) repairBestEffort() error {
	offset := uint64(walHeaderSize)
	writeOffset := offset
	repaired := false
	for offset < s.used {
		rec, err := s.recordAtOffset(offset, true)
		if err != nil {
			next, ok := s.nextValidRecord(offset + 1)
			if !ok {
				repaired = true
				break
			}
			repaired = true
			offset = next
			continue
		}

		recordSize := rec.end - offset
		if writeOffset != offset {
			copy(s.data[writeOffset:writeOffset+recordSize], s.data[offset:rec.end])
			repaired = true
		}
		writeOffset += recordSize
		offset = rec.end
	}
	if !repaired {
		return nil
	}
	if err := s.truncate(writeOffset); err != nil {
		return err
	}
	return s.Sync()
}

func (s *mmapWALRecordStore) nextValidRecord(start uint64) (uint64, bool) {
	for offset := start; offset+walRecordHeaderSize <= s.used; offset++ {
		if _, err := s.recordAtOffset(offset, true); err == nil {
			return offset, true
		}
	}
	return 0, false
}

func (s *mmapWALRecordStore) truncate(used uint64) error {
	if used < walHeaderSize || used > s.size {
		return ErrCorruptWAL
	}
	s.used = used
	s.writeUsed()
	return nil
}

func (s *mmapWALRecordStore) Sync() error {
	if s.dataDirty {
		if err := msyncMmap(s.data); err != nil {
			return err
		}
		s.dataDirty = false
	}
	if s.metadataDirty {
		if err := syncMmapFileMetadata(s.file); err != nil {
			return err
		}
		s.metadataDirty = false
	}
	return nil
}

func (s *mmapWALRecordStore) Close() error {
	firstErr := s.Sync()
	if err := s.closeAfterSync(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *mmapWALRecordStore) closeAfterSync() error {
	var firstErr error
	if s.data != nil {
		if err := syscall.Munmap(s.data); err != nil && firstErr == nil {
			firstErr = err
		}
		s.data = nil
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}
	return firstErr
}

func (s *mmapWALRecordStore) appendRecord(op byte, key, value []byte) (minpatricia.Position, error) {
	if s.sealed {
		return 0, ErrWalSealed
	}
	keyLen := len(key)
	valueLen := len(value)
	total := uint64(walRecordHeaderSize) + uint64(keyLen) + uint64(valueLen)
	limit := min(s.size, recordOffsetLimit)
	if s.used+total > limit {
		return 0, ErrWalFull
	}

	start := s.used
	record := s.data[start : start+total]
	record[walRecordOpOffset] = op
	binary.LittleEndian.PutUint32(record[walRecordKeyOffset:walRecordKeyOffset+4], uint32(keyLen))
	binary.LittleEndian.PutUint32(record[walRecordValueOffset:walRecordValueOffset+4], uint32(valueLen))
	copy(record[walRecordHeaderSize:], key)
	copy(record[walRecordHeaderSize+keyLen:], value)
	binary.LittleEndian.PutUint32(record[walRecordCRCOffset:walRecordCRCOffset+4], walRecordCRC(record))

	s.used += total
	s.writeUsed()
	return makeRecordPosition(s.fileNo, start)
}

func (s *mmapWALRecordStore) recordAt(pos minpatricia.Position, verifyCRC bool) (walRecord, error) {
	if recordPositionFileNo(pos) != s.fileNo {
		return walRecord{}, ErrCorruptWAL
	}
	return s.recordAtOffset(recordPositionOffset(pos), verifyCRC)
}

func (s *mmapWALRecordStore) recordAtOffset(offset uint64, verifyCRC bool) (walRecord, error) {
	if offset < walHeaderSize || offset+walRecordHeaderSize > s.used {
		return walRecord{}, ErrCorruptWAL
	}
	header := s.data[offset : offset+walRecordHeaderSize]
	op := header[walRecordOpOffset]
	switch op {
	case walOpPut, walOpDelete, walOpInstallSST, walOpInstallSSTBatch, walOpWriteBatch:
	case walBatchEntryPut:
		if verifyCRC {
			return walRecord{}, ErrCorruptWAL
		}
		op = walOpPut
	case walBatchEntryDelete:
		if verifyCRC {
			return walRecord{}, ErrCorruptWAL
		}
		op = walOpDelete
	default:
		return walRecord{}, ErrCorruptWAL
	}
	keyLen := uint64(binary.LittleEndian.Uint32(header[walRecordKeyOffset : walRecordKeyOffset+4]))
	valueLen := uint64(binary.LittleEndian.Uint32(header[walRecordValueOffset : walRecordValueOffset+4]))
	switch op {
	case walOpPut:
		if keyLen > minpatricia.MaxKeySize {
			return walRecord{}, ErrCorruptWAL
		}
	case walOpDelete:
		if keyLen > minpatricia.MaxKeySize || valueLen != 0 {
			return walRecord{}, ErrCorruptWAL
		}
	case walOpInstallSST:
		if keyLen != walInstallSSTPayloadSize || valueLen != 0 {
			return walRecord{}, ErrCorruptWAL
		}
	case walOpInstallSSTBatch, walOpWriteBatch:
		if valueLen != 0 {
			return walRecord{}, ErrCorruptWAL
		}
	}
	end := offset + walRecordHeaderSize + keyLen + valueLen
	if end < offset || end > s.used {
		return walRecord{}, ErrCorruptWAL
	}
	record := s.data[offset:end]
	switch op {
	case walOpInstallSSTBatch:
		if _, _, err := validateInstallSSTBatchPayload(record[walRecordHeaderSize:]); err != nil {
			return walRecord{}, err
		}
	case walOpWriteBatch:
		if err := forEachWriteBatchPayloadEntry(record[walRecordHeaderSize:], nil); err != nil {
			return walRecord{}, err
		}
	}
	if verifyCRC {
		wantCRC := binary.LittleEndian.Uint32(header[walRecordCRCOffset : walRecordCRCOffset+4])
		if gotCRC := walRecordCRC(record); gotCRC != wantCRC {
			return walRecord{}, ErrCorruptWAL
		}
	}

	keyStart := offset + walRecordHeaderSize
	valueStart := keyStart + keyLen
	return walRecord{
		op:    op,
		key:   s.data[keyStart:valueStart],
		value: s.data[valueStart:end],
		end:   end,
	}, nil
}

func (s *mmapWALRecordStore) initHeader() {
	copy(s.data[:8], walHeaderMagic[:])
	binary.LittleEndian.PutUint32(s.data[walHeaderVersionOffset:walHeaderVersionOffset+4], walVersion)
	s.used = walHeaderSize
	s.writeUsed()
}

func (s *mmapWALRecordStore) loadHeader() error {
	if !bytes.Equal(s.data[:8], walHeaderMagic[:]) {
		return ErrCorruptWAL
	}
	if version := binary.LittleEndian.Uint32(s.data[walHeaderVersionOffset : walHeaderVersionOffset+4]); version != walVersion {
		return ErrCorruptWAL
	}
	used := binary.LittleEndian.Uint64(s.data[walHeaderUsedOffset : walHeaderUsedOffset+8])
	if used < walHeaderSize || used > s.size {
		return ErrCorruptWAL
	}
	s.used = used
	return nil
}

func (s *mmapWALRecordStore) writeUsed() {
	binary.LittleEndian.PutUint64(s.data[walHeaderUsedOffset:walHeaderUsedOffset+8], s.used)
	s.dataDirty = true
}

type walRecord struct {
	op    byte
	key   []byte
	value []byte
	end   uint64
}

func walRecordCRC(record []byte) uint32 {
	crc := crc32.NewIEEE()
	_, _ = crc.Write(record[walRecordOpOffset:walRecordCRCOffset])
	_, _ = crc.Write(record[walRecordHeaderSize:])
	return crc.Sum32()
}
