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

const mmapNodeStoreCopyWorkers = 4

func copyMmapNodeStoreDir(src, dst string) error {
	return copyMmapNodeStoreDirWithWorkers(src, dst, mmapNodeStoreCopyWorkers)
}

func copyMmapNodeStoreDirWithWorkers(src, dst string, workers int) error {
	srcNames, err := mmapNodeStoreExtentNames(src)
	if err != nil {
		return err
	}
	targetCreated, err := ensureMmapNodeStoreCopyTarget(dst)
	if err != nil {
		return err
	}
	dirChanged := false

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
			dirChanged = true
			continue
		}
		if _, err := parseMmapNodeExtentID(name); err != nil {
			return err
		}
		if _, ok := srcSet[name]; !ok {
			if err := os.Remove(filepath.Join(dst, name)); err != nil {
				return err
			}
			dirChanged = true
		}
	}
	updatedDir, err := syncMmapNodeExtentFiles(src, dst, srcNames, workers)
	if err != nil {
		return err
	}
	dirChanged = dirChanged || updatedDir
	if dirChanged {
		if err := syncDir(dst); err != nil {
			return err
		}
	}
	if targetCreated {
		return syncDir(filepath.Dir(dst))
	}
	return nil
}

type mmapNodeExtentSyncResult struct {
	updatedDir bool
	err        error
}

func syncMmapNodeExtentFiles(src, dst string, names []string, workers int) (bool, error) {
	workers = boundedWorkers(workers, len(names))
	if workers == 0 {
		return false, nil
	}
	if workers == 1 {
		dirChanged := false
		for _, name := range names {
			updatedDir, err := syncMmapNodeExtentFile(filepath.Join(src, name), filepath.Join(dst, name))
			if err != nil {
				return false, err
			}
			dirChanged = dirChanged || updatedDir
		}
		return dirChanged, nil
	}
	jobs := make(chan string)
	results := make(chan mmapNodeExtentSyncResult, len(names))
	for i := 0; i < workers; i++ {
		go func() {
			for name := range jobs {
				updatedDir, err := syncMmapNodeExtentFile(filepath.Join(src, name), filepath.Join(dst, name))
				results <- mmapNodeExtentSyncResult{updatedDir: updatedDir, err: err}
			}
		}()
	}
	go func() {
		for _, name := range names {
			jobs <- name
		}
		close(jobs)
	}()

	dirChanged := false
	var firstErr error
	for range names {
		result := <-results
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		dirChanged = dirChanged || result.updatedDir
	}
	if firstErr != nil {
		return false, firstErr
	}
	return dirChanged, nil
}

func boundedWorkers(workers, jobs int) int {
	if jobs == 0 {
		return 0
	}
	if workers < 1 {
		workers = 1
	}
	if workers > jobs {
		return jobs
	}
	return workers
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

func ensureMmapNodeStoreCopyTarget(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, ErrManifest
	}
	return false, nil
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

func syncMmapNodeExtentFile(src, dst string) (bool, error) {
	if err := requireMmapNodeExtentPath(src); err != nil {
		return false, err
	}
	if _, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
		if err := copyMmapNodeExtentFile(src, dst); err != nil {
			return false, err
		}
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, updateMmapNodeExtentFile(src, dst)
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

func updateMmapNodeExtentFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	if err := requireMmapNodeExtentFile(srcFile); err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	if err := requireMmapNodeExtentFile(dstFile); err != nil {
		return err
	}

	srcData, err := syscall.Mmap(int(srcFile.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(srcData)
	dstData, err := syscall.Mmap(int(dstFile.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(dstData)

	dirty := false
	reservedEnd := mmapNodeReservedPages * mmapNodePageSize
	if !bytes.Equal(srcData[:reservedEnd], dstData[:reservedEnd]) {
		copy(dstData[:reservedEnd], srcData[:reservedEnd])
		dirty = true
	}
	bitmap := srcData[mmapNodePageSize : mmapNodePageSize*2]
	pagesDirty := copyDifferentMmapNodeUsedPages(srcData, dstData, bitmap)
	dirty = dirty || pagesDirty
	if !dirty {
		return nil
	}
	if err := msyncMmap(dstData); err != nil {
		return err
	}
	if err := syncMmapFileMetadata(dstFile); err != nil {
		return err
	}
	return nil
}

// Used pages are usually allocated in contiguous runs. Compare the whole run
// first so equal checkpoints skip quickly; copy individual pages only when a
// run differs.
func copyDifferentMmapNodeUsedPages(src, dst, bitmap []byte) bool {
	dirty := false
	var runStart uint64
	var runLen uint64
	flushRun := func() {
		if runLen == 0 {
			return
		}
		if copyDifferentMmapNodeUsedPageRun(src, dst, runStart, runLen) {
			dirty = true
		}
		runLen = 0
	}
	for slot := uint64(0); slot < mmapNodeSlotsPerExtent; slot++ {
		if mmapNodeBitmapUsed(bitmap, slot) {
			if runLen == 0 {
				runStart = slot
			}
			runLen++
			continue
		}
		flushRun()
	}
	flushRun()
	return dirty
}

func copyDifferentMmapNodeUsedPageRun(src, dst []byte, startSlot, slots uint64) bool {
	offset := (mmapNodeReservedPages + int(startSlot)) * mmapNodePageSize
	end := offset + int(slots)*mmapNodePageSize
	if bytes.Equal(src[offset:end], dst[offset:end]) {
		return false
	}

	dirty := false
	for slot := startSlot; slot < startSlot+slots; slot++ {
		offset := (mmapNodeReservedPages + int(slot)) * mmapNodePageSize
		end := offset + mmapNodePageSize
		if bytes.Equal(src[offset:end], dst[offset:end]) {
			continue
		}
		copy(dst[offset:end], src[offset:end])
		dirty = true
	}
	return dirty
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
