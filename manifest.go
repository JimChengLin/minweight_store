package minweight_store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	manifestName           = "MANIFEST"
	manifestVersion uint32 = 2
	manifestSize           = 40

	manifestVersionOffset         = 0
	manifestCheckpointWALNoOffset = 4
	manifestActiveWALNoOffset     = 12
	manifestNextWALNoOffset       = 20
	manifestWALSegmentSizeOffset  = 28
	manifestCRCOffset             = 36
)

type manifest struct {
	path string
}

// manifestState is the payload portion of MANIFEST.
//
// The complete on-disk manifest is:
//
//	version || manifestState fields || crc32(version || manifestState fields)
//
// It is not mutable in-memory store state. Code builds a fresh manifestState
// when checkpoint progress changes, then writes that payload with the current
// manifest version and CRC.
type manifestState struct {
	checkpointWALFileNo uint64
	activeWALFileNo     uint64
	nextWALFileNo       uint64
	walSegmentSize      uint64
}

func (m *manifest) read() (manifestState, bool, error) {
	return readManifest(m.path)
}

func (m *manifest) write(state manifestState) error {
	return writeManifest(m.path, state)
}

func (m *manifest) dir() string {
	return filepath.Dir(m.path)
}

// readManifest returns the checked payload when MANIFEST exists. The bool is
// false only when the file is absent; corrupt or incompatible bytes are errors.
func readManifest(path string) (manifestState, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return manifestState{}, false, nil
	}
	if err != nil {
		return manifestState{}, false, err
	}
	if len(data) != manifestSize {
		return manifestState{}, false, ErrManifest
	}
	if version := binary.LittleEndian.Uint32(data[manifestVersionOffset : manifestVersionOffset+4]); version != manifestVersion {
		return manifestState{}, false, ErrManifest
	}
	wantCRC := binary.LittleEndian.Uint32(data[manifestCRCOffset : manifestCRCOffset+4])
	if gotCRC := crc32.ChecksumIEEE(data[:manifestCRCOffset]); gotCRC != wantCRC {
		return manifestState{}, false, ErrManifest
	}
	state := manifestState{
		checkpointWALFileNo: binary.LittleEndian.Uint64(data[manifestCheckpointWALNoOffset : manifestCheckpointWALNoOffset+8]),
		activeWALFileNo:     binary.LittleEndian.Uint64(data[manifestActiveWALNoOffset : manifestActiveWALNoOffset+8]),
		nextWALFileNo:       binary.LittleEndian.Uint64(data[manifestNextWALNoOffset : manifestNextWALNoOffset+8]),
		walSegmentSize:      binary.LittleEndian.Uint64(data[manifestWALSegmentSizeOffset : manifestWALSegmentSizeOffset+8]),
	}
	if err := validateManifestState(state); err != nil {
		return manifestState{}, false, err
	}
	return state, true, nil
}

// writeManifest atomically writes version + payload + crc. The caller owns when
// a new manifestState becomes durable checkpoint progress.
func writeManifest(path string, state manifestState) error {
	if err := validateManifestState(state); err != nil {
		return err
	}
	data := make([]byte, manifestSize)
	binary.LittleEndian.PutUint32(data[manifestVersionOffset:manifestVersionOffset+4], manifestVersion)
	binary.LittleEndian.PutUint64(data[manifestCheckpointWALNoOffset:manifestCheckpointWALNoOffset+8], state.checkpointWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestActiveWALNoOffset:manifestActiveWALNoOffset+8], state.activeWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestNextWALNoOffset:manifestNextWALNoOffset+8], state.nextWALFileNo)
	binary.LittleEndian.PutUint64(data[manifestWALSegmentSizeOffset:manifestWALSegmentSizeOffset+8], state.walSegmentSize)
	binary.LittleEndian.PutUint32(data[manifestCRCOffset:manifestCRCOffset+4], crc32.ChecksumIEEE(data[:manifestCRCOffset]))

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
