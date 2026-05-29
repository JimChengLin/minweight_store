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
	walDirName           = "wal"
	sstDirName           = "sst"
	walSegmentSuffix     = ".wal"
	parquetSegmentSuffix = ".parquet"
	firstWALSegmentNo    = 1
)

type recordSegment interface {
	Key(pos minpatricia.Position) ([]byte, bool)
	Value(pos minpatricia.Position) ([]byte, bool)
	Close() error
	closeAfterSync() error
}

type segmentedRecordStore struct {
	rootDir      string
	size         int64
	mu           sync.RWMutex
	segments     map[uint64]recordSegment
	active       *mmapWALRecordStore
	activeFileNo uint64
	nextFileNo   uint64
	// Parquet segment creation syncs sst/ in parquetRecordStore.Sync().
	walDirDirty bool
}

func openSegmentedRecordStore(dir string, size int64, activeFileNo, nextFileNo uint64) (*segmentedRecordStore, error) {
	walDir := walSegmentsPath(dir)
	ids, err := listWALSegmentIDs(walDir)
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
	if nextFileNo <= activeFileNo {
		return nil, ErrManifest
	}

	store := &segmentedRecordStore{
		rootDir:      dir,
		size:         size,
		segments:     make(map[uint64]recordSegment, len(ids)),
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
		path := filepath.Join(walDir, walSegmentName(id))
		openSize, missing, err := walSegmentOpenSize(path, size)
		if err != nil {
			return nil, err
		}
		wal, err := openMmapWALRecordStore(path, openSize, id)
		if err != nil {
			return nil, err
		}
		wal.sealed = id != activeFileNo
		store.segments[id] = wal
		if id == activeFileNo {
			store.active = wal
		}
		if missing {
			store.walDirDirty = true
		}
	}
	if err := store.openParquetSegments(); err != nil {
		return nil, err
	}
	storeOwnedByCaller = true
	return store, nil
}

func createRecordSegmentDirs(dir string) error {
	if err := os.MkdirAll(walSegmentsPath(dir), 0o755); err != nil {
		return err
	}
	return os.MkdirAll(sstSegmentsPath(dir), 0o755)
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

func (s *segmentedRecordStore) AppendInstallSSTRecord(sourceWALFileNo, sstFileNo uint64) (minpatricia.Position, error) {
	active := s.activeSegment()
	if active == nil {
		return 0, ErrClosed
	}
	return active.AppendInstallSSTRecord(sourceWALFileNo, sstFileNo)
}

func (s *segmentedRecordStore) Free(pos minpatricia.Position) error {
	return nil
}

func (s *segmentedRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	segment := s.segment(recordPositionFileNo(pos))
	if segment == nil {
		return nil, false
	}
	return segment.Key(pos)
}

func (s *segmentedRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	segment := s.segment(recordPositionFileNo(pos))
	if segment == nil {
		return nil, false
	}
	return segment.Value(pos)
}

func (s *segmentedRecordStore) Sync() error {
	active := s.activeSegment()
	if active == nil {
		return ErrClosed
	}
	if err := active.Sync(); err != nil {
		return err
	}
	return s.syncDirIfDirty()
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
	if s.walDirDirty {
		if err := syncDir(walSegmentsPath(s.rootDir)); err != nil && firstErr == nil {
			firstErr = err
		}
		s.walDirDirty = false
	}
	s.segments = nil
	s.active = nil
	s.activeFileNo = 0
	s.nextFileNo = 0
	return firstErr
}

func (s *segmentedRecordStore) closeAfterSync() error {
	var firstErr error
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, segment := range s.segments {
		if err := segment.closeAfterSync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.segments = nil
	s.active = nil
	s.activeFileNo = 0
	s.nextFileNo = 0
	return firstErr
}

func (s *segmentedRecordStore) Rollover() (*mmapWALRecordStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newFileNo := s.nextFileNo
	old := s.active
	if old == nil {
		return nil, ErrClosed
	}
	wal, err := openMmapWALRecordStore(filepath.Join(walSegmentsPath(s.rootDir), walSegmentName(newFileNo)), s.size, newFileNo)
	if err != nil {
		return nil, err
	}

	old.sealed = true
	s.segments[newFileNo] = wal
	s.active = wal
	s.activeFileNo = newFileNo
	s.nextFileNo = newFileNo + 1
	s.walDirDirty = true
	return old, nil
}

func (s *segmentedRecordStore) ReplayWAL(fileNo uint64, policy WALReplayPolicy, fn func(op byte, key []byte, pos minpatricia.Position) error) error {
	wal := s.walSegment(fileNo)
	if wal == nil {
		return ErrCorruptWAL
	}
	return wal.Replay(policy, fn)
}

func (s *segmentedRecordStore) repairWALBestEffort(fileNo uint64) (bool, error) {
	wal := s.walSegment(fileNo)
	if wal == nil {
		return false, ErrCorruptWAL
	}
	return wal.repairBestEffort()
}

func (s *segmentedRecordStore) createParquetSegment() (*parquetRecordStore, error) {
	s.mu.Lock()
	fileNo := s.nextFileNo
	if s.segments[fileNo] != nil {
		s.mu.Unlock()
		return nil, ErrManifest
	}
	s.nextFileNo++
	s.mu.Unlock()

	return createParquetRecordStore(parquetSegmentPath(s.rootDir, fileNo), fileNo)
}

func (s *segmentedRecordStore) installParquetSegment(store *parquetRecordStore) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileNo := store.fileNo
	if s.segments[fileNo] != nil {
		return ErrManifest
	}
	s.segments[fileNo] = store
	return nil
}

func (s *segmentedRecordStore) parquetSegment(fileNo uint64) (*parquetRecordStore, error) {
	s.mu.RLock()
	segment := s.segments[fileNo]
	s.mu.RUnlock()
	if parquetSegment, ok := segment.(*parquetRecordStore); ok {
		return parquetSegment, nil
	}
	if segment != nil {
		return nil, ErrManifest
	}

	store, err := openParquetRecordStore(parquetSegmentPath(s.rootDir, fileNo), fileNo)
	if err != nil {
		return nil, err
	}
	storeOwnedByMap := false
	defer func() {
		if !storeOwnedByMap {
			_ = store.Close()
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.segments[fileNo] != nil {
		return nil, ErrManifest
	}
	s.segments[fileNo] = store
	// WAL replay can install a parquet created after the last manifest commit,
	// so advance the allocator past that recovered file number.
	if s.nextFileNo <= fileNo {
		s.nextFileNo = fileNo + 1
	}
	storeOwnedByMap = true
	return store, nil
}

func (s *segmentedRecordStore) compactableWALFileNos(checkpointWALFileNo uint64, keep int) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]uint64, 0, len(s.segments))
	for fileNo, segment := range s.segments {
		if fileNo <= checkpointWALFileNo {
			if _, ok := segment.(*mmapWALRecordStore); ok {
				ids = append(ids, fileNo)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	if len(ids) <= keep {
		return nil
	}
	return ids[:len(ids)-keep]
}

func (s *segmentedRecordStore) segment(fileNo uint64) recordSegment {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.segments[fileNo]
}

func (s *segmentedRecordStore) walSegment(fileNo uint64) *mmapWALRecordStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	wal, _ := s.segments[fileNo].(*mmapWALRecordStore)
	return wal
}

func (s *segmentedRecordStore) activeSegment() *mmapWALRecordStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.active
}

func (s *segmentedRecordStore) syncDirIfDirty() error {
	s.mu.Lock()
	if !s.walDirDirty {
		s.mu.Unlock()
		return nil
	}
	s.walDirDirty = false
	s.mu.Unlock()

	if err := syncDir(walSegmentsPath(s.rootDir)); err != nil {
		s.mu.Lock()
		s.walDirDirty = true
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *segmentedRecordStore) openParquetSegments() error {
	sstDir := sstSegmentsPath(s.rootDir)
	ids, err := listParquetSegmentIDs(sstDir)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id >= s.nextFileNo {
			continue
		}
		if s.segments[id] != nil {
			return ErrManifest
		}
		store, err := openParquetRecordStore(parquetSegmentPath(s.rootDir, id), id)
		if err != nil {
			return err
		}
		s.segments[id] = store
	}
	return nil
}

func walSegmentOpenSize(path string, configuredSize int64) (int64, bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return configuredSize, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	return info.Size(), false, nil
}

func listWALSegmentIDs(dir string) ([]uint64, error) {
	return listRecordSegmentIDs(dir, walSegmentSuffix)
}

func listParquetSegmentIDs(dir string) ([]uint64, error) {
	return listRecordSegmentIDs(dir, parquetSegmentSuffix)
}

func listRecordSegmentIDs(dir, suffix string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}
		id, err := parseRecordSegmentID(entry.Name(), suffix)
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

func sstSegmentsPath(dir string) string {
	return filepath.Join(dir, sstDirName)
}

func parquetSegmentPath(dir string, fileNo uint64) string {
	return filepath.Join(sstSegmentsPath(dir), parquetSegmentName(fileNo))
}

func parquetSegmentName(fileNo uint64) string {
	return fmt.Sprintf("%020d%s", fileNo, parquetSegmentSuffix)
}

func parseWALSegmentID(name string) (uint64, error) {
	return parseRecordSegmentID(name, walSegmentSuffix)
}

func parseRecordSegmentID(name, suffix string) (uint64, error) {
	if len(name) != 20+len(suffix) || !strings.HasSuffix(name, suffix) {
		return 0, fmt.Errorf("minweight_store: invalid record segment name %q", name)
	}
	return strconv.ParseUint(strings.TrimSuffix(name, suffix), 10, 64)
}
