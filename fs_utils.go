//go:build darwin || linux

package minweight_store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
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

	if err := copyMmapNodeExtentFiles(src, dst, srcNames, workers); err != nil {
		return err
	}
	if len(srcNames) != 0 {
		dirChanged = true
	}
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

func copyMmapNodeExtentFiles(src, dst string, names []string, workers int) error {
	workers = boundedWorkers(workers, len(names))
	if workers == 0 {
		return nil
	}
	if workers == 1 {
		for _, name := range names {
			if err := copyOrReplaceMmapNodeExtentFile(filepath.Join(src, name), filepath.Join(dst, name)); err != nil {
				return err
			}
		}
		return nil
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	hasErr := func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr != nil
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range jobs {
				if hasErr() {
					continue
				}
				if err := copyOrReplaceMmapNodeExtentFile(filepath.Join(src, name), filepath.Join(dst, name)); err != nil {
					setErr(err)
				}
			}
		}()
	}
	for _, name := range names {
		if hasErr() {
			break
		}
		jobs <- name
	}
	close(jobs)
	wg.Wait()
	return firstErr
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

func copyOrReplaceMmapNodeExtentFile(src, dst string) error {
	if err := requireMmapNodeExtentPath(src); err != nil {
		return err
	}
	if _, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
		return copyMmapNodeExtentFile(src, dst)
	} else if err != nil {
		return err
	}
	if err := requireMmapNodeExtentPath(dst); err != nil {
		return err
	}
	return replaceMmapNodeExtentFile(src, dst)
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
	if err := cloneMmapNodeExtentFile(src, dst); err == nil {
		return nil
	} else if !errors.Is(err, errFSCloneUnsupported) {
		return err
	}
	return copyMmapNodeExtentFileSparse(src, dst)
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

func replaceMmapNodeExtentFile(src, dst string) error {
	tmp := dst + mmapNodeExtentCopyTempSuffix
	if err := cloneMmapNodeExtentFile(src, tmp); err != nil {
		if !errors.Is(err, errFSCloneUnsupported) {
			_ = os.Remove(tmp)
			return err
		}
		if err := copyMmapNodeExtentFileFull(src, tmp); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func copyMmapNodeExtentFileFull(src, dst string) error {
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
	written, err := io.Copy(out, in)
	if err != nil {
		return err
	}
	if written != mmapNodeExtentBytes {
		return ErrManifest
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
		if bitsetGet(bitmap, slot) {
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
