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
	manifestName                    = "MANIFEST"
	manifestVersion          uint32 = 6
	manifestSize                    = 1 << 20
	manifestRecordHeaderSize        = 64
	manifestLiveSSTEntrySize        = 24

	manifestVersionOffset           = 0
	manifestRecordSizeOffset        = 4
	manifestCheckpointWALNoOffset   = 8
	manifestActiveWALNoOffset       = 16
	manifestNextFileNoOffset        = 24
	manifestWALSegmentSizeOffset    = 32
	manifestPrimaryWALFlushedOffset = 40
	manifestLiveSSTCountOffset      = 44
	manifestSeqOffset               = 48
	manifestCRCOffset               = 56
	manifestLiveSSTOffset           = manifestRecordHeaderSize
)

type manifest struct {
	path       string
	file       *os.File
	nextSeq    uint64
	nextOffset int
}

// manifestState is the payload portion of MANIFEST.
//
// The complete on-disk manifest is:
//
//	header || live_sst_entry[]
//
// MANIFEST is a 1MiB variable-size log. Normal writes append the next record
// and fsync the file. When the next record would not fit, write compacts by
// replacing MANIFEST with a fresh file containing only the newest record.
//
// It is not mutable in-memory store state. Code builds a fresh manifestState
// when checkpoint progress changes, then writes that payload with the current
// manifest version and CRC.
type manifestState struct {
	checkpointWALFileNo uint64
	activeWALFileNo     uint64
	nextFileNo          uint64
	walSegmentSize      uint64
	primaryWALFlushed   bool
	liveSSTs            []manifestLiveSST
}

type liveSSTStats struct {
	totalEntries   uint64
	deletedEntries uint64
}

type manifestLiveSST struct {
	fileNo uint64
	liveSSTStats
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

	m.nextSeq = record.seq + 1
	m.nextOffset = record.offset + record.size
	return m, record.state, true, nil
}

func (m *manifest) write(state manifestState) error {
	if m.nextSeq == 0 {
		return ErrManifest
	}
	seq, offset := m.nextSeq, m.nextOffset
	record, err := encodeManifestRecord(state, seq)
	if err != nil {
		return err
	}
	if m.file == nil || offset+len(record) > manifestSize {
		if err := m.close(); err != nil {
			return err
		}
		if err := replaceManifestRecord(m.path, record); err != nil {
			return err
		}
		file, err := os.OpenFile(m.path, os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		m.file = file
		offset = 0
	} else {
		if err := appendManifestRecord(m.file, record, offset); err != nil {
			return err
		}
	}
	m.nextSeq = seq + 1
	m.nextOffset = offset + len(record)
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

type manifestRecord struct {
	state  manifestState
	seq    uint64
	offset int
	size   int
}

type manifestRecordMeta struct {
	size         int
	liveSSTCount int
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
	for offset := 0; offset+manifestRecordHeaderSize <= manifestSize; {
		if isZeroBytes(data[offset : offset+manifestRecordHeaderSize]) {
			break
		}
		record, recordSize, ok := decodeManifestRecord(data[offset:], offset)
		if !ok {
			if recordSize == 0 {
				break
			}
			offset += recordSize
			continue
		}
		if record.seq > latest.seq {
			latest = record
		}
		offset += record.size
	}
	if latest.seq == 0 {
		return manifestRecord{}, ErrManifest
	}
	return latest, nil
}

func manifestRecordHeader(data []byte) (manifestRecordMeta, bool) {
	if len(data) < manifestRecordHeaderSize {
		return manifestRecordMeta{}, false
	}
	if version := binary.LittleEndian.Uint32(data[manifestVersionOffset : manifestVersionOffset+4]); version != manifestVersion {
		return manifestRecordMeta{}, false
	}
	recordSize := int(binary.LittleEndian.Uint32(data[manifestRecordSizeOffset : manifestRecordSizeOffset+4]))
	if recordSize < manifestRecordHeaderSize || recordSize > len(data) {
		return manifestRecordMeta{}, false
	}
	liveSSTCount := int(binary.LittleEndian.Uint32(data[manifestLiveSSTCountOffset : manifestLiveSSTCountOffset+4]))
	if recordSize != manifestRecordHeaderSize+liveSSTCount*manifestLiveSSTEntrySize {
		return manifestRecordMeta{}, false
	}
	return manifestRecordMeta{size: recordSize, liveSSTCount: liveSSTCount}, true
}

func decodeManifestRecord(data []byte, offset int) (manifestRecord, int, bool) {
	meta, ok := manifestRecordHeader(data)
	if !ok {
		return manifestRecord{}, 0, false
	}
	recordData := data[:meta.size]
	wantCRC := binary.LittleEndian.Uint32(data[manifestCRCOffset : manifestCRCOffset+4])
	if gotCRC := manifestRecordCRC(recordData); gotCRC != wantCRC {
		return manifestRecord{}, meta.size, false
	}
	primaryWALFlushed := binary.LittleEndian.Uint32(data[manifestPrimaryWALFlushedOffset : manifestPrimaryWALFlushedOffset+4])
	if primaryWALFlushed > 1 {
		return manifestRecord{}, meta.size, false
	}
	liveSSTs := make([]manifestLiveSST, meta.liveSSTCount)
	for i := range liveSSTs {
		start := manifestLiveSSTOffset + i*manifestLiveSSTEntrySize
		liveSSTs[i] = manifestLiveSST{
			fileNo: binary.LittleEndian.Uint64(data[start : start+8]),
			liveSSTStats: liveSSTStats{
				totalEntries:   binary.LittleEndian.Uint64(data[start+8 : start+16]),
				deletedEntries: binary.LittleEndian.Uint64(data[start+16 : start+24]),
			},
		}
	}
	state := manifestState{
		checkpointWALFileNo: binary.LittleEndian.Uint64(data[manifestCheckpointWALNoOffset : manifestCheckpointWALNoOffset+8]),
		activeWALFileNo:     binary.LittleEndian.Uint64(data[manifestActiveWALNoOffset : manifestActiveWALNoOffset+8]),
		nextFileNo:          binary.LittleEndian.Uint64(data[manifestNextFileNoOffset : manifestNextFileNoOffset+8]),
		walSegmentSize:      binary.LittleEndian.Uint64(data[manifestWALSegmentSizeOffset : manifestWALSegmentSizeOffset+8]),
		primaryWALFlushed:   primaryWALFlushed == 1,
		liveSSTs:            liveSSTs,
	}
	if err := validateManifestState(state); err != nil {
		return manifestRecord{}, meta.size, false
	}
	seq := binary.LittleEndian.Uint64(data[manifestSeqOffset : manifestSeqOffset+8])
	if seq == 0 {
		return manifestRecord{}, meta.size, false
	}
	return manifestRecord{state: state, seq: seq, offset: offset, size: meta.size}, meta.size, true
}

func encodeManifestRecord(state manifestState, seq uint64) ([]byte, error) {
	if err := validateManifestState(state); err != nil {
		return nil, err
	}
	recordSize := manifestRecordHeaderSize + len(state.liveSSTs)*manifestLiveSSTEntrySize
	if recordSize > manifestSize {
		return nil, ErrManifest
	}
	data := make([]byte, recordSize)
	binary.LittleEndian.PutUint32(data[manifestVersionOffset:manifestVersionOffset+4], manifestVersion)
	binary.LittleEndian.PutUint32(data[manifestRecordSizeOffset:manifestRecordSizeOffset+4], uint32(recordSize))
	binary.LittleEndian.PutUint64(data[manifestCheckpointWALNoOffset:manifestCheckpointWALNoOffset+8], state.checkpointWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestActiveWALNoOffset:manifestActiveWALNoOffset+8], state.activeWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestNextFileNoOffset:manifestNextFileNoOffset+8], state.nextFileNo)
	binary.LittleEndian.PutUint64(data[manifestWALSegmentSizeOffset:manifestWALSegmentSizeOffset+8], state.walSegmentSize)
	if state.primaryWALFlushed {
		binary.LittleEndian.PutUint32(data[manifestPrimaryWALFlushedOffset:manifestPrimaryWALFlushedOffset+4], 1)
	}
	binary.LittleEndian.PutUint32(data[manifestLiveSSTCountOffset:manifestLiveSSTCountOffset+4], uint32(len(state.liveSSTs)))
	binary.LittleEndian.PutUint64(data[manifestSeqOffset:manifestSeqOffset+8], seq)
	for i, sst := range state.liveSSTs {
		start := manifestLiveSSTOffset + i*manifestLiveSSTEntrySize
		binary.LittleEndian.PutUint64(data[start:start+8], sst.fileNo)
		binary.LittleEndian.PutUint64(data[start+8:start+16], sst.totalEntries)
		binary.LittleEndian.PutUint64(data[start+16:start+24], sst.deletedEntries)
	}
	binary.LittleEndian.PutUint32(data[manifestCRCOffset:manifestCRCOffset+4], manifestRecordCRC(data))
	return data, nil
}

func manifestRecordCRC(data []byte) uint32 {
	crc := crc32.Update(0, crc32.IEEETable, data[:manifestCRCOffset])
	var zeroCRC [4]byte
	crc = crc32.Update(crc, crc32.IEEETable, zeroCRC[:])
	return crc32.Update(crc, crc32.IEEETable, data[manifestCRCOffset+4:])
}

func appendManifestRecord(file *os.File, record []byte, offset int) error {
	if _, err := file.WriteAt(record, int64(offset)); err != nil {
		return err
	}
	return file.Sync()
}

func replaceManifestRecord(path string, record []byte) error {
	if len(record) > manifestSize {
		return ErrManifest
	}
	data := make([]byte, manifestSize)
	copy(data, record)

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	removeTmp := true
	defer func() {
		if file != nil {
			_ = file.Close()
		}
		if removeTmp {
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
	file = nil
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

func validateManifestState(state manifestState) error {
	if state.checkpointWALFileNo == 0 || state.activeWALFileNo <= state.checkpointWALFileNo || state.nextFileNo <= state.activeWALFileNo {
		return ErrManifest
	}
	if state.walSegmentSize < walHeaderSize+walRecordHeaderSize || state.walSegmentSize > recordOffsetLimit {
		return ErrManifest
	}
	for i, sst := range state.liveSSTs {
		fileNo := sst.fileNo
		if !validRecordFileNo(fileNo) || fileNo >= state.nextFileNo {
			return ErrManifest
		}
		if sst.deletedEntries > sst.totalEntries || sst.totalEntries > recordOffsetLimit {
			return ErrManifest
		}
		if i != 0 && state.liveSSTs[i-1].fileNo >= fileNo {
			return ErrManifest
		}
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
