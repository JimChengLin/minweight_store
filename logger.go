package minweight_store

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	logName              = "LOG"
	defaultLogMaxSize    = 64 << 20
	defaultLogMaxBackups = 8
)

type rotatingLogWriter struct {
	dir        string
	maxSize    int64
	maxBackups int
	mu         sync.Mutex
	file       *os.File
	size       int64
}

func openStoreLogger(dir string, logger *slog.Logger) (*slog.Logger, *rotatingLogWriter, error) {
	if logger != nil {
		return logger, nil, nil
	}
	writer, err := openRotatingLogWriter(dir, defaultLogMaxSize, defaultLogMaxBackups)
	if err != nil {
		return nil, nil, err
	}
	return slog.New(slog.NewTextHandler(writer, nil)), writer, nil
}

func openRotatingLogWriter(dir string, maxSize int64, maxBackups int) (*rotatingLogWriter, error) {
	path := filepath.Join(dir, logName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fileOwnedByWriter := false
	defer func() {
		if !fileOwnedByWriter {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	writer := &rotatingLogWriter{
		dir:        dir,
		maxSize:    maxSize,
		maxBackups: maxBackups,
		file:       file,
		size:       info.Size(),
	}
	fileOwnedByWriter = true
	return writer, nil
}

func (w *rotatingLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, os.ErrClosed
	}
	if w.maxSize > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxSize {
		if err := w.rotateWithWriterLocked(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *rotatingLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *rotatingLogWriter) rotateWithWriterLocked() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	archivePath, err := w.nextArchivePath()
	if err != nil {
		return err
	}
	activePath := filepath.Join(w.dir, logName)
	if err := os.Rename(activePath, archivePath); err != nil {
		return err
	}
	file, err := os.OpenFile(activePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.size = 0
	return w.deleteOldArchives()
}

func (w *rotatingLogWriter) nextArchivePath() (string, error) {
	name := logName + "." + time.Now().Format("20060102-150405.000000") + "." + strconv.Itoa(os.Getpid())
	return filepath.Join(w.dir, name), nil
}

func (w *rotatingLogWriter) deleteOldArchives() error {
	if w.maxBackups <= 0 {
		return nil
	}
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	type archive struct {
		name    string
		modTime time.Time
	}
	archives := make([]archive, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), logName+".") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		archives = append(archives, archive{
			name:    entry.Name(),
			modTime: info.ModTime(),
		})
	}
	sort.Slice(archives, func(i, j int) bool {
		if archives[i].modTime.Equal(archives[j].modTime) {
			return archives[i].name < archives[j].name
		}
		return archives[i].modTime.Before(archives[j].modTime)
	})
	for len(archives) > w.maxBackups {
		if err := os.Remove(filepath.Join(w.dir, archives[0].name)); err != nil {
			return err
		}
		archives = archives[1:]
	}
	return nil
}

func logInfo(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Info(msg, args...)
	}
}

func logError(logger *slog.Logger, msg string, err error, args ...any) {
	if logger == nil {
		return
	}
	args = append(args, "err", err)
	logger.Error(msg, args...)
}
