//go:build darwin || linux

package minweight_store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"syscall"

	"github.com/JimChengLin/minpatricia"
)

const (
	defaultWALSize int64 = 64 * 1024 * 1024

	walVersion uint32 = 1

	walHeaderVersionOffset = 8
	walHeaderUsedOffset    = 16

	walRecordHeaderSize  = 13
	walRecordOpOffset    = 0
	walRecordKeyOffset   = 1
	walRecordValueOffset = 5
	walRecordCRCOffset   = 9

	walOpPut    = 1
	walOpDelete = 2
)

var walHeaderMagic = [8]byte{'M', 'W', 'W', 'A', 'L', '0', '1', 0}

// WALReplayPolicy controls how Open handles corrupt WAL records during replay.
type WALReplayPolicy uint8

const (
	// WALReplayStrict fails Open on the first corrupt WAL record.
	WALReplayStrict WALReplayPolicy = iota
	// WALReplayPointInTime replays the valid prefix and truncates the WAL there.
	WALReplayPointInTime
	// WALReplayBestEffort skips corrupt bytes and scans for later CRC-valid records.
	WALReplayBestEffort
)

type mmapWALRecordStore struct {
	file *os.File
	data []byte
	size uint64
	used uint64
}

func openMmapWALRecordStore(path string, size int64) (*mmapWALRecordStore, error) {
	if size < walHeaderSize+walRecordHeaderSize {
		return nil, ErrWalFull
	}
	if uint64(size)&minpatriciaHandleTag != 0 {
		return nil, minpatricia.ErrPositionTag
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
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
	if info.Size() == 0 {
		if err := file.Truncate(size); err != nil {
			return nil, err
		}
	} else if info.Size() != size {
		return nil, errors.Join(ErrCorruptWAL, errors.New("wal size does not match configured size"))
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	store := &mmapWALRecordStore{
		file: file,
		data: data,
		size: uint64(size),
	}
	if isZeroBytes(data[:walHeaderSize]) {
		store.initHeader()
	} else if err := store.loadHeader(); err != nil {
		_ = syscall.Munmap(data)
		return nil, err
	}
	ok = true
	return store, nil
}

func (s *mmapWALRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	return s.appendRecord(walOpPut, key, value)
}

func (s *mmapWALRecordStore) Delete(key []byte) (minpatricia.Position, error) {
	return s.appendRecord(walOpDelete, key, nil)
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
		return s.replayBestEffort(fn)
	default:
		return ErrReplayPolicy
	}
}

func (s *mmapWALRecordStore) replayStrict(fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	offset := uint64(walHeaderSize)
	for offset < s.used {
		rec, err := s.recordAt(minpatricia.Position(offset), true)
		if err != nil {
			return err
		}
		if err := fn(rec.op, rec.key, minpatricia.Position(offset)); err != nil {
			return err
		}
		offset = rec.end
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
		rec, err := s.recordAt(minpatricia.Position(offset), true)
		if err != nil {
			return s.truncate(lastGoodOffset)
		}
		if err := fn(rec.op, rec.key, minpatricia.Position(offset)); err != nil {
			return err
		}
		offset = rec.end
		lastGoodOffset = offset
	}
	return nil
}

func (s *mmapWALRecordStore) replayBestEffort(fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	offset := uint64(walHeaderSize)
	for offset < s.used {
		rec, err := s.recordAt(minpatricia.Position(offset), true)
		if err != nil {
			next, ok := s.nextValidRecord(offset + 1)
			if !ok {
				return nil
			}
			offset = next
			continue
		}
		if err := fn(rec.op, rec.key, minpatricia.Position(offset)); err != nil {
			return err
		}
		offset = rec.end
	}
	return nil
}

func (s *mmapWALRecordStore) nextValidRecord(start uint64) (uint64, bool) {
	for offset := start; offset+walRecordHeaderSize <= s.used; offset++ {
		if _, err := s.recordAt(minpatricia.Position(offset), true); err == nil {
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
	if err := msyncMmap(s.data); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *mmapWALRecordStore) Close() error {
	var firstErr error
	if s.data != nil {
		if err := msyncMmap(s.data); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := syscall.Munmap(s.data); err != nil && firstErr == nil {
			firstErr = err
		}
		s.data = nil
	}
	if s.file != nil {
		if err := s.file.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}
	return firstErr
}

func (s *mmapWALRecordStore) appendRecord(op byte, key, value []byte) (minpatricia.Position, error) {
	if op != walOpPut && op != walOpDelete {
		return 0, ErrCorruptWAL
	}
	if op == walOpDelete {
		value = nil
	}
	if len(key) > minpatricia.MaxKeySize {
		return 0, minpatricia.ErrKeyTooLarge
	}
	keyLen := len(key)
	valueLen := len(value)
	if uint64(keyLen) > uint64(^uint32(0)) || uint64(valueLen) > uint64(^uint32(0)) {
		return 0, ErrWalFull
	}
	total := uint64(walRecordHeaderSize + keyLen + valueLen)
	if total > s.size-s.used {
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
	return minpatricia.Position(start), nil
}

func (s *mmapWALRecordStore) recordAt(pos minpatricia.Position, verifyCRC bool) (walRecord, error) {
	offset := uint64(pos)
	if offset < walHeaderSize || offset+walRecordHeaderSize > s.used {
		return walRecord{}, ErrCorruptWAL
	}
	header := s.data[offset : offset+walRecordHeaderSize]
	op := header[walRecordOpOffset]
	if op != walOpPut && op != walOpDelete {
		return walRecord{}, ErrCorruptWAL
	}
	keyLen := uint64(binary.LittleEndian.Uint32(header[walRecordKeyOffset : walRecordKeyOffset+4]))
	valueLen := uint64(binary.LittleEndian.Uint32(header[walRecordValueOffset : walRecordValueOffset+4]))
	if keyLen > minpatricia.MaxKeySize {
		return walRecord{}, ErrCorruptWAL
	}
	if op == walOpDelete && valueLen != 0 {
		return walRecord{}, ErrCorruptWAL
	}
	end := offset + walRecordHeaderSize + keyLen + valueLen
	if end < offset || end > s.used {
		return walRecord{}, ErrCorruptWAL
	}
	record := s.data[offset:end]
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
	if string(s.data[:8]) != string(walHeaderMagic[:]) {
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

func isZeroBytes(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
