//go:build darwin

package minweight_store

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func cloneMmapNodeExtentFile(src, dst string) error {
	if err := unix.Clonefile(src, dst, 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) ||
			errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOSYS) {
			_ = os.Remove(dst)
			return errFSCloneUnsupported
		}
		return err
	}
	return syncClonedMmapNodeExtentFile(dst)
}

func syncClonedMmapNodeExtentFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}
