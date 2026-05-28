//go:build darwin || linux

package minweight_store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/JimChengLin/minpatricia"
)

const (
	walDirName        = "wal"
	walSegmentSuffix  = ".wal"
	firstWALSegmentNo = 1
)

type segmentedRecordStore struct {
	dir          string
	size         int64
	mu           sync.RWMutex
	segments     map[uint64]*mmapWALRecordStore
	activeFileNo uint64
	nextFileNo   uint64
}

func openSegmentedRecordStore(dir string, size int64, activeFileNo, nextFileNo uint64) (*segmentedRecordStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	ids, err := listWALSegmentIDs(dir)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		if activeFileNo != 0 {
			return nil, ErrManifest
		}
		if nextFileNo != 0 {
			return nil, ErrManifest
		}
		activeFileNo = firstWALSegmentNo
		nextFileNo = activeFileNo + 1
		ids = append(ids, activeFileNo)
	} else if activeFileNo == 0 {
		return nil, ErrManifest
	}
	if ids[len(ids)-1] != activeFileNo {
		return nil, ErrManifest
	}
	if nextFileNo != activeFileNo+1 {
		return nil, ErrManifest
	}

	store := &segmentedRecordStore{
		dir:          dir,
		size:         size,
		segments:     make(map[uint64]*mmapWALRecordStore, len(ids)),
		activeFileNo: activeFileNo,
		nextFileNo:   nextFileNo,
	}
	storeOwnedByCaller := false
	defer func() {
		if !storeOwnedByCaller {
			_ = store.Close()
		}
	}()

	for _, id := range ids {
		path := filepath.Join(dir, walSegmentName(id))
		openSize, err := existingWALSegmentSize(path, size)
		if err != nil {
			return nil, err
		}
		wal, err := openMmapWALRecordStore(path, openSize, id)
		if err != nil {
			return nil, err
		}
		wal.sealed = id != activeFileNo
		store.segments[id] = wal
	}
	storeOwnedByCaller = true
	return store, nil
}

func (s *segmentedRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	active := s.activeSegment()
	if active == nil {
		return 0, ErrClosed
	}
	return active.Append(key, value)
}

func (s *segmentedRecordStore) Delete(key []byte) (minpatricia.Position, error) {
	active := s.activeSegment()
	if active == nil {
		return 0, ErrClosed
	}
	return active.Delete(key)
}

func (s *segmentedRecordStore) Free(pos minpatricia.Position) error {
	return nil
}

func (s *segmentedRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	wal := s.segment(recordPositionFileNo(pos))
	if wal == nil {
		return nil, false
	}
	return wal.Key(pos)
}

func (s *segmentedRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	wal := s.segment(recordPositionFileNo(pos))
	if wal == nil {
		return nil, false
	}
	return wal.Value(pos)
}

func (s *segmentedRecordStore) Sync() error {
	active := s.activeSegment()
	if active == nil {
		return ErrClosed
	}
	return active.Sync()
}

func (s *segmentedRecordStore) Close() error {
	var firstErr error
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, segment := range s.segments {
		if err := segment.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.segments = nil
	s.activeFileNo = 0
	s.nextFileNo = 0
	return firstErr
}

func (s *segmentedRecordStore) Rollover() (*mmapWALRecordStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newFileNo := s.nextFileNo
	old := s.segments[s.activeFileNo]
	if old == nil {
		return nil, ErrClosed
	}
	wal, err := openMmapWALRecordStore(filepath.Join(s.dir, walSegmentName(newFileNo)), s.size, newFileNo)
	if err != nil {
		return nil, err
	}

	old.sealed = true
	s.segments[newFileNo] = wal
	s.activeFileNo = newFileNo
	s.nextFileNo = newFileNo + 1
	return old, nil
}

func (s *segmentedRecordStore) ReplayWAL(fileNo uint64, policy WALReplayPolicy, fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	wal := s.segment(fileNo)
	if wal == nil {
		return ErrCorruptWAL
	}
	return wal.Replay(policy, fn)
}

func (s *segmentedRecordStore) repairWALBestEffort(fileNo uint64) (bool, error) {
	wal := s.segment(fileNo)
	if wal == nil {
		return false, ErrCorruptWAL
	}
	return wal.repairBestEffort()
}

func (s *segmentedRecordStore) segment(fileNo uint64) *mmapWALRecordStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.segments[fileNo]
}

func (s *segmentedRecordStore) activeSegment() *mmapWALRecordStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.segments[s.activeFileNo]
}

func existingWALSegmentSize(path string, configuredSize int64) (int64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return configuredSize, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func listWALSegmentIDs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), walSegmentSuffix) {
			continue
		}
		id, err := parseWALSegmentID(entry.Name())
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}

func walSegmentName(fileNo uint64) string {
	return fmt.Sprintf("%020d%s", fileNo, walSegmentSuffix)
}

func parseWALSegmentID(name string) (uint64, error) {
	if len(name) != len("00000000000000000000.wal") || !strings.HasSuffix(name, walSegmentSuffix) {
		return 0, fmt.Errorf("minweight_store: invalid wal segment name %q", name)
	}
	return strconv.ParseUint(strings.TrimSuffix(name, walSegmentSuffix), 10, 64)
}
