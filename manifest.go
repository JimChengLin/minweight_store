package minweight_store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	manifestName              = "MANIFEST"
	manifestVersion    uint32 = 4
	manifestSize              = 4096
	manifestRecordSize        = 64
	manifestSlotCount         = manifestSize / manifestRecordSize

	manifestVersionOffset           = 0
	manifestCheckpointWALNoOffset   = 4
	manifestActiveWALNoOffset       = 12
	manifestNextWALNoOffset         = 20
	manifestWALSegmentSizeOffset    = 28
	manifestPrimaryWALFlushedOffset = 36
	manifestSeqOffset               = 40
	manifestCRCOffset               = 48
)

type manifest struct {
	path     string
	file     *os.File
	nextSeq  uint64
	nextSlot int
}

// manifestState is the payload portion of MANIFEST.
//
// The complete on-disk manifest is:
//
//	version || checkpoint_wal_file_no || active_wal_file_no ||
//	next_wal_file_no || wal_segment_size || primary_wal_flushed ||
//	seq || crc32(all previous bytes)
//
// MANIFEST is a 4KiB fixed-size log of 64-byte records. Normal writes append
// the next slot and fsync the file. When the log is full, write compacts by
// replacing MANIFEST with a fresh file containing only the newest record.
//
// It is not mutable in-memory store state. Code builds a fresh manifestState
// when checkpoint progress changes, then writes that payload with the current
// manifest version and CRC.
type manifestState struct {
	checkpointWALFileNo uint64
	activeWALFileNo     uint64
	nextWALFileNo       uint64
	walSegmentSize      uint64
	primaryWALFlushed   bool
}

func openManifest(path string) (*manifest, manifestState, bool, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if errors.Is(err, os.ErrNotExist) {
		return &manifest{path: path, nextSeq: 1}, manifestState{}, false, nil
	}
	if err != nil {
		return nil, manifestState{}, false, err
	}

	m := &manifest{path: path, file: file}
	record, err := readManifestLogFile(file)
	if err != nil {
		_ = m.close()
		return nil, manifestState{}, false, err
	}

	m.nextSeq, m.nextSlot = nextManifestWrite(record.seq, record.slot)
	return m, record.state, true, nil
}

func (m *manifest) write(state manifestState) error {
	if err := validateManifestState(state); err != nil {
		return err
	}
	if m.nextSeq == 0 {
		return ErrManifest
	}
	seq, slot := m.nextSeq, m.nextSlot
	if slot == 0 {
		if err := m.close(); err != nil {
			return err
		}
		file, err := replaceManifestAndOpen(m.path, state, seq)
		if err != nil {
			return err
		}
		m.file = file
	} else {
		if m.file == nil {
			return ErrManifest
		}
		if err := appendManifestRecord(m.file, state, seq, slot); err != nil {
			return err
		}
	}
	m.nextSeq, m.nextSlot = nextManifestWrite(seq, slot)
	return nil
}

func (m *manifest) dir() string {
	return filepath.Dir(m.path)
}

func (m *manifest) close() error {
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

// readManifest returns the latest checked payload when MANIFEST exists. The
// bool is false only when the file is absent; files with no valid record are
// errors.
func readManifest(path string) (manifestState, bool, error) {
	record, ok, err := readManifestLog(path)
	if err != nil || !ok {
		return manifestState{}, ok, err
	}
	return record.state, true, nil
}

type manifestRecord struct {
	state manifestState
	seq   uint64
	slot  int
}

func readManifestLog(path string) (manifestRecord, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return manifestRecord{}, false, nil
	}
	if err != nil {
		return manifestRecord{}, false, err
	}
	defer file.Close()
	record, err := readManifestLogFile(file)
	if err != nil {
		return manifestRecord{}, false, err
	}
	return record, true, nil
}

func readManifestLogFile(file *os.File) (manifestRecord, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return manifestRecord{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return manifestRecord{}, err
	}
	if info.Size() != manifestSize {
		return manifestRecord{}, ErrManifest
	}
	var data [manifestSize]byte
	if _, err := io.ReadFull(file, data[:]); err != nil {
		return manifestRecord{}, err
	}

	var latest manifestRecord
	found := false
	for slot := 0; slot < manifestSlotCount; slot++ {
		recordData := data[slot*manifestRecordSize : (slot+1)*manifestRecordSize]
		if manifestRecordEmpty(recordData) {
			continue
		}
		record, ok := decodeManifestRecord(recordData, slot)
		if !ok {
			continue
		}
		if !found || record.seq > latest.seq {
			latest = record
			found = true
		}
	}
	if !found {
		return manifestRecord{}, ErrManifest
	}
	return latest, nil
}

func decodeManifestRecord(data []byte, slot int) (manifestRecord, bool) {
	if version := binary.LittleEndian.Uint32(data[manifestVersionOffset : manifestVersionOffset+4]); version != manifestVersion {
		return manifestRecord{}, false
	}
	wantCRC := binary.LittleEndian.Uint32(data[manifestCRCOffset : manifestCRCOffset+4])
	if gotCRC := crc32.ChecksumIEEE(data[:manifestCRCOffset]); gotCRC != wantCRC {
		return manifestRecord{}, false
	}
	primaryWALFlushed := binary.LittleEndian.Uint32(data[manifestPrimaryWALFlushedOffset : manifestPrimaryWALFlushedOffset+4])
	if primaryWALFlushed > 1 {
		return manifestRecord{}, false
	}
	state := manifestState{
		checkpointWALFileNo: binary.LittleEndian.Uint64(data[manifestCheckpointWALNoOffset : manifestCheckpointWALNoOffset+8]),
		activeWALFileNo:     binary.LittleEndian.Uint64(data[manifestActiveWALNoOffset : manifestActiveWALNoOffset+8]),
		nextWALFileNo:       binary.LittleEndian.Uint64(data[manifestNextWALNoOffset : manifestNextWALNoOffset+8]),
		walSegmentSize:      binary.LittleEndian.Uint64(data[manifestWALSegmentSizeOffset : manifestWALSegmentSizeOffset+8]),
		primaryWALFlushed:   primaryWALFlushed == 1,
	}
	if err := validateManifestState(state); err != nil {
		return manifestRecord{}, false
	}
	seq := binary.LittleEndian.Uint64(data[manifestSeqOffset : manifestSeqOffset+8])
	if seq == 0 {
		return manifestRecord{}, false
	}
	return manifestRecord{state: state, seq: seq, slot: slot}, true
}

func manifestRecordEmpty(data []byte) bool {
	return isZeroBytes(data)
}

// writeManifest commits version + payload + seq + crc. The caller owns when a
// new manifestState becomes durable checkpoint progress.
func writeManifest(path string, state manifestState) error {
	if err := validateManifestState(state); err != nil {
		return err
	}
	latest, ok, err := readManifestLog(path)
	if err != nil {
		return err
	}
	if !ok {
		return replaceManifest(path, state, 1)
	}
	seq, slot := nextManifestWrite(latest.seq, latest.slot)
	if slot == 0 {
		return replaceManifest(path, state, seq)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	var firstErr error
	firstErr = appendManifestRecord(file, state, seq, slot)
	if err := file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func nextManifestWrite(seq uint64, slot int) (uint64, int) {
	seq++
	slot++
	if slot >= manifestSlotCount {
		slot = 0
	}
	return seq, slot
}

func encodeManifestRecord(state manifestState, seq uint64) [manifestRecordSize]byte {
	var data [manifestRecordSize]byte
	binary.LittleEndian.PutUint32(data[manifestVersionOffset:manifestVersionOffset+4], manifestVersion)
	binary.LittleEndian.PutUint64(data[manifestCheckpointWALNoOffset:manifestCheckpointWALNoOffset+8], state.checkpointWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestActiveWALNoOffset:manifestActiveWALNoOffset+8], state.activeWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestNextWALNoOffset:manifestNextWALNoOffset+8], state.nextWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestWALSegmentSizeOffset:manifestWALSegmentSizeOffset+8], state.walSegmentSize)
	if state.primaryWALFlushed {
		binary.LittleEndian.PutUint32(data[manifestPrimaryWALFlushedOffset:manifestPrimaryWALFlushedOffset+4], 1)
	}
	binary.LittleEndian.PutUint64(data[manifestSeqOffset:manifestSeqOffset+8], seq)
	binary.LittleEndian.PutUint32(data[manifestCRCOffset:manifestCRCOffset+4], crc32.ChecksumIEEE(data[:manifestCRCOffset]))
	return data
}

func appendManifestRecord(file *os.File, state manifestState, seq uint64, slot int) error {
	record := encodeManifestRecord(state, seq)
	if _, err := file.WriteAt(record[:], int64(slot*manifestRecordSize)); err != nil {
		return err
	}
	return file.Sync()
}

func replaceManifestAndOpen(path string, state manifestState, seq uint64) (*os.File, error) {
	if err := replaceManifest(path, state, seq); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR, 0o600)
}

func replaceManifest(path string, state manifestState, seq uint64) error {
	data := make([]byte, manifestSize)
	record := encodeManifestRecord(state, seq)
	copy(data, record[:])

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	committed := false
	closed := false
	defer func() {
		if !committed {
			if !closed {
				_ = file.Close()
			}
			_ = os.Remove(tmp)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	committed = true
	return nil
}

func validateManifestState(state manifestState) error {
	if state.checkpointWALFileNo == 0 || state.activeWALFileNo != state.checkpointWALFileNo+1 || state.nextWALFileNo != state.activeWALFileNo+1 {
		return ErrManifest
	}
	if state.walSegmentSize < walHeaderSize+walRecordHeaderSize || state.walSegmentSize > recordOffsetLimit {
		return ErrManifest
	}
	return nil
}

func syncDir(dir string) (err error) {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return file.Sync()
}
