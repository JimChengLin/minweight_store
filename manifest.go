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
	manifestVersion uint32 = 1
	manifestSize           = 16

	manifestVersionOffset = 0
	manifestWALUsedOffset = 4
	manifestCRCOffset     = 12
)

type manifest struct {
	path string
}

func (m *manifest) read() (uint64, bool, error) {
	return readManifest(m.path)
}

func (m *manifest) write(walUsedBytes uint64) error {
	return writeManifest(m.path, walUsedBytes)
}

func (m *manifest) remove() error {
	return removeManifest(m.path)
}

func readManifest(path string) (uint64, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(data) != manifestSize {
		return 0, false, ErrManifest
	}
	if version := binary.LittleEndian.Uint32(data[manifestVersionOffset : manifestVersionOffset+4]); version != manifestVersion {
		return 0, false, ErrManifest
	}
	wantCRC := binary.LittleEndian.Uint32(data[manifestCRCOffset : manifestCRCOffset+4])
	if gotCRC := crc32.ChecksumIEEE(data[:manifestCRCOffset]); gotCRC != wantCRC {
		return 0, false, ErrManifest
	}
	walUsedBytes := binary.LittleEndian.Uint64(data[manifestWALUsedOffset : manifestWALUsedOffset+8])
	if walUsedBytes < walHeaderSize {
		return 0, false, ErrManifest
	}
	return walUsedBytes, true, nil
}

func writeManifest(path string, walUsedBytes uint64) error {
	if walUsedBytes < walHeaderSize {
		return ErrManifest
	}
	data := make([]byte, manifestSize)
	binary.LittleEndian.PutUint32(data[manifestVersionOffset:manifestVersionOffset+4], manifestVersion)
	binary.LittleEndian.PutUint64(data[manifestWALUsedOffset:manifestWALUsedOffset+8], walUsedBytes)
	binary.LittleEndian.PutUint32(data[manifestCRCOffset:manifestCRCOffset+4], crc32.ChecksumIEEE(data[:manifestCRCOffset]))

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
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
	ok = true
	return nil
}

func removeManifest(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
