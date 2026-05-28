//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

var errFSCloneUnsupported = errors.New("minweight_store: filesystem clone unsupported")

const mmapNodeExtentCopyTempSuffix = ".copytmp"

func copyMmapNodeStoreDir(src, dst string) error {
	srcNames, err := mmapNodeStoreExtentNames(src)
	if err != nil {
		return err
	}
	if err := ensureMmapNodeStoreCopyTarget(dst); err != nil {
		return err
	}

	srcSet := make(map[string]struct{}, len(srcNames))
	for _, name := range srcNames {
		srcSet[name] = struct{}{}
	}

	dstEntries, err := os.ReadDir(dst)
	if err != nil {
		return err
	}
	for _, entry := range dstEntries {
		if entry.IsDir() {
			return ErrManifest
		}
		name := entry.Name()
		if isMmapNodeExtentCopyTempName(name) {
			if err := os.Remove(filepath.Join(dst, name)); err != nil {
				return err
			}
			continue
		}
		if _, err := parseMmapNodeExtentID(name); err != nil {
			return err
		}
		if _, ok := srcSet[name]; !ok {
			if err := os.Remove(filepath.Join(dst, name)); err != nil {
				return err
			}
		}
	}
	for _, name := range srcNames {
		srcPath := filepath.Join(src, name)
		dstPath := filepath.Join(dst, name)
		equal, err := mmapNodeExtentFilesEqual(srcPath, dstPath)
		if err != nil {
			return err
		}
		if equal {
			continue
		}
		if err := replaceMmapNodeExtentFile(srcPath, dstPath); err != nil {
			return err
		}
	}
	return syncDir(dst)
}

func copyMmapNodeStoreDirIfDifferent(src, dst string) error {
	equal, err := mmapNodeStoreDirsEqual(src, dst)
	if err != nil {
		return err
	}
	if equal {
		return nil
	}
	return copyMmapNodeStoreDir(src, dst)
}

func mmapNodeStoreDirsEqual(src, dst string) (bool, error) {
	srcNames, err := mmapNodeStoreExtentNames(src)
	if err != nil {
		return false, err
	}
	dstNames, ok, err := maybeMmapNodeStoreExtentNames(dst)
	if err != nil {
		return false, err
	}
	if !ok || len(srcNames) != len(dstNames) {
		return false, nil
	}
	for i, name := range srcNames {
		if name != dstNames[i] {
			return false, nil
		}
		equal, err := mmapNodeExtentFilesEqual(filepath.Join(src, name), filepath.Join(dst, name))
		if err != nil || !equal {
			return equal, err
		}
	}
	return true, nil
}

func mmapNodeStoreExtentNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, ErrManifest
		}
		if _, err := parseMmapNodeExtentID(entry.Name()); err != nil {
			return nil, err
		}
		names = append(names, entry.Name())
	}
	return names, nil
}

func maybeMmapNodeStoreExtentNames(dir string) ([]string, bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, false, nil
		}
		if _, err := parseMmapNodeExtentID(entry.Name()); err != nil {
			return nil, false, nil
		}
		names = append(names, entry.Name())
	}
	return names, true, nil
}

func requireMmapNodeStoreDir(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrManifest
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return ErrManifest
	}
	return nil
}

func ensureMmapNodeStoreCopyTarget(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return ErrManifest
	}
	return nil
}

func copyMmapNodeExtentFile(src, dst string) error {
	if err := requireMmapNodeExtentPath(src); err != nil {
		return err
	}
	if err := cloneMmapNodeExtentFile(src, dst); err == nil {
		return nil
	} else if !errors.Is(err, errFSCloneUnsupported) {
		return err
	}
	return copyMmapNodeExtentFileSparse(src, dst)
}

func replaceMmapNodeExtentFile(src, dst string) error {
	tmp := dst + mmapNodeExtentCopyTempSuffix
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := copyMmapNodeExtentFile(src, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func isMmapNodeExtentCopyTempName(name string) bool {
	if len(name) <= len(mmapNodeExtentCopyTempSuffix) {
		return false
	}
	if name[len(name)-len(mmapNodeExtentCopyTempSuffix):] != mmapNodeExtentCopyTempSuffix {
		return false
	}
	_, err := parseMmapNodeExtentID(name[:len(name)-len(mmapNodeExtentCopyTempSuffix)])
	return err == nil
}

func mmapNodeExtentFilesEqual(src, dst string) (bool, error) {
	srcFile, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer srcFile.Close()
	if err := requireMmapNodeExtentFile(srcFile); err != nil {
		return false, err
	}

	dstFile, err := os.Open(dst)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer dstFile.Close()
	if err := requireMmapNodeExtentFile(dstFile); err != nil {
		if errors.Is(err, ErrManifest) {
			return false, nil
		}
		return false, err
	}

	srcData, err := syscall.Mmap(int(srcFile.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return false, err
	}
	defer syscall.Munmap(srcData)
	dstData, err := syscall.Mmap(int(dstFile.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return false, err
	}
	defer syscall.Munmap(dstData)

	reservedEnd := mmapNodeReservedPages * mmapNodePageSize
	if !bytes.Equal(srcData[:reservedEnd], dstData[:reservedEnd]) {
		return false, nil
	}
	bitmap := srcData[mmapNodePageSize : mmapNodePageSize*2]
	return mmapNodeUsedPagesEqual(srcData, dstData, bitmap)
}

func mmapNodeUsedPagesEqual(src, dst, bitmap []byte) (bool, error) {
	var runStart uint64
	var runLen uint64
	compareRun := func() bool {
		if runLen == 0 {
			return true
		}
		offset := (mmapNodeReservedPages + int(runStart)) * mmapNodePageSize
		end := offset + int(runLen)*mmapNodePageSize
		runLen = 0
		return bytes.Equal(src[offset:end], dst[offset:end])
	}

	for slot := uint64(0); slot < mmapNodeSlotsPerExtent; slot++ {
		if mmapNodeBitmapUsed(bitmap, slot) {
			if runLen == 0 {
				runStart = slot
			}
			runLen++
			continue
		}
		if !compareRun() {
			return false, nil
		}
	}
	return compareRun(), nil
}

func requireMmapNodeExtentPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() != mmapNodeExtentBytes {
		return ErrManifest
	}
	return nil
}

func requireMmapNodeExtentFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != mmapNodeExtentBytes {
		return ErrManifest
	}
	return nil
}

func copyMmapNodeExtentFileSparse(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	copyComplete := false
	defer func() {
		if !copyComplete {
			_ = out.Close()
			_ = os.Remove(dst)
		}
	}()
	if err := out.Truncate(mmapNodeExtentBytes); err != nil {
		return err
	}

	reserved := make([]byte, mmapNodeReservedPages*mmapNodePageSize)
	if _, err := io.ReadFull(io.NewSectionReader(in, 0, int64(len(reserved))), reserved); err != nil {
		return err
	}
	if _, err := out.WriteAt(reserved, 0); err != nil {
		return err
	}
	bitmap := reserved[mmapNodePageSize : mmapNodePageSize*2]

	var runStart uint64
	var runLen uint64
	flushRun := func() error {
		if runLen == 0 {
			return nil
		}
		offset := int64(mmapNodeReservedPages+runStart) * mmapNodePageSize
		length := int64(runLen) * mmapNodePageSize
		runLen = 0
		section := io.NewSectionReader(in, offset, length)
		if _, err := out.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		_, err := io.CopyN(out, section, length)
		return err
	}
	for slot := uint64(0); slot < mmapNodeSlotsPerExtent; slot++ {
		if mmapNodeBitmapUsed(bitmap, slot) {
			if runLen == 0 {
				runStart = slot
			}
			runLen++
			continue
		}
		if err := flushRun(); err != nil {
			return err
		}
	}
	if err := flushRun(); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	copyComplete = true
	return nil
}
