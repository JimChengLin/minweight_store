//go:build darwin || linux

package minweight_store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

const storeLockName = "LOCK"

type storeFileLock struct {
	file *os.File
}

func lockStoreDir(dir string) (*storeFileLock, error) {
	file, err := os.OpenFile(filepath.Join(dir, storeLockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	lockAcquired := false
	defer func() {
		if !lockAcquired {
			_ = file.Close()
		}
	}()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, err
	}
	lockAcquired = true
	return &storeFileLock{file: file}, nil
}

func (l *storeFileLock) Close() error {
	firstErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	if err := l.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	l.file = nil
	return firstErr
}
