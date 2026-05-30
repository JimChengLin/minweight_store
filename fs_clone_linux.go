//go:build linux

package minweight_store

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func cloneMmapNodeExtentFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = out.Close()
			_ = os.Remove(dst)
		}
	}()

	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) ||
			errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOSYS) ||
			errors.Is(err, unix.EINVAL) {
			return errFSCloneUnsupported
		}
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}
