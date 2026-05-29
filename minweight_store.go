package minweight_store

import (
	"bytes"
	"errors"
	"sync"
)

var (
	ErrInvalidRange = errors.New("minweight_store: invalid range")
	ErrCorruptIndex = errors.New("minweight_store: index points to missing record")
	ErrWalFull      = errors.New("minweight_store: wal is full")
	ErrWalSealed    = errors.New("minweight_store: wal segment is sealed")
	ErrCorruptWAL   = errors.New("minweight_store: corrupt wal")
	ErrClosed       = errors.New("minweight_store: store is closed")
	ErrFatal        = errors.New("minweight_store: store is fatal")
	ErrReplayPolicy = errors.New("minweight_store: invalid wal replay policy")
	ErrManifest     = errors.New("minweight_store: corrupt manifest")
	ErrParquet      = errors.New("minweight_store: invalid parquet record store")
)

type Store struct {
	secondaryIndexMu    sync.Mutex
	primaryMu           sync.RWMutex
	backend             *indexBackend
	manifest            *manifest
	records             *segmentedRecordStore
	checkpointWALFileNo uint64
	fatal               error
}

type Item struct {
	Key   []byte
	Value []byte
}

type VisitFunc func(Item) bool

func New() *Store {
	return &Store{
		backend: newIndexBackend(),
	}
}

func (s *Store) Len() int {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	if s.backend == nil || s.fatal != nil {
		return 0
	}
	return s.backend.len()
}

func (s *Store) Put(key, value []byte) error {
	for {
		s.primaryMu.Lock()
		backend, err := s.openBackend()
		if err != nil {
			s.primaryMu.Unlock()
			return err
		}
		result, err := backend.put(key, value)
		walFullNotAccepted := errors.Is(err, ErrWalFull) && result == backendMutationNotAccepted
		canFlush := false
		if s.records != nil && s.manifest != nil {
			active := s.records.activeSegment()
			canFlush = active != nil && active.used != walHeaderSize
		}
		s.primaryMu.Unlock()

		if walFullNotAccepted {
			if !canFlush {
				return err
			}
			if flushErr := s.flush(); flushErr != nil {
				return s.mayMarkFatal(flushErr)
			}
			continue
		}
		if err != nil && result == backendMutationAcceptedThenFailed {
			return s.mayMarkFatal(err)
		}
		return err
	}
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return nil, false, err
	}
	return backend.get(key)
}

func (s *Store) Delete(key []byte) (bool, error) {
	for {
		s.primaryMu.Lock()
		backend, err := s.openBackend()
		if err != nil {
			s.primaryMu.Unlock()
			return false, err
		}
		deleted, result, err := backend.delete(key)
		walFullNotAccepted := errors.Is(err, ErrWalFull) && result == backendMutationNotAccepted
		canFlush := false
		if s.records != nil && s.manifest != nil {
			active := s.records.activeSegment()
			canFlush = active != nil && active.used != walHeaderSize
		}
		s.primaryMu.Unlock()

		if walFullNotAccepted {
			if !canFlush {
				return deleted, err
			}
			if flushErr := s.flush(); flushErr != nil {
				return deleted, s.mayMarkFatal(flushErr)
			}
			continue
		}
		if err != nil && result == backendMutationAcceptedThenFailed {
			return deleted, s.mayMarkFatal(err)
		}
		return deleted, err
	}
}

func (s *Store) Scan(fn VisitFunc) error {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.scan(fn)
}

func (s *Store) ScanRange(greaterOrEqual, lessThan []byte, fn VisitFunc) error {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.scanRange(greaterOrEqual, lessThan, fn)
}

func (s *Store) ReverseScan(fn VisitFunc) error {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.reverseScan(fn)
}

func (s *Store) ReverseScanRange(lessOrEqual, greaterThan []byte, fn VisitFunc) error {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.reverseScanRange(lessOrEqual, greaterThan, fn)
}

func (s *Store) SeekGE(key []byte) (Item, bool, error) {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return Item{}, false, err
	}
	return backend.seekGE(key)
}

func (s *Store) SeekLE(key []byte) (Item, bool, error) {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return Item{}, false, err
	}
	return backend.seekLE(key)
}

func (s *Store) Close() error {
	s.secondaryIndexMu.Lock()
	defer s.secondaryIndexMu.Unlock()

	s.primaryMu.Lock()
	if s.backend == nil {
		s.primaryMu.Unlock()
		return nil
	}

	firstErr := s.fatal
	synced := false
	if firstErr == nil {
		if err := s.closeCheckpointWithPrimaryLocked(); err != nil {
			firstErr = errors.Join(ErrFatal, err)
			s.fatal = firstErr
		} else {
			synced = true
		}
	}

	backend := s.backend
	manifest := s.manifest
	s.backend = nil
	s.manifest = nil
	s.records = nil
	s.primaryMu.Unlock()

	var closeErr error
	if synced {
		closeErr = backend.closeAfterSync()
	} else {
		closeErr = backend.close()
	}
	if closeErr != nil {
		firstErr = errors.Join(firstErr, closeErr)
	}
	if manifest != nil {
		if err := manifest.close(); err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}
	return firstErr
}

func (s *Store) openBackend() (*indexBackend, error) {
	if s.backend == nil {
		return nil, ErrClosed
	}
	if s.fatal != nil {
		return nil, s.fatal
	}
	return s.backend, nil
}

func (s *Store) flush() error {
	s.secondaryIndexMu.Lock()
	defer s.secondaryIndexMu.Unlock()

	return s.flushWithSecondaryLocked()
}

func (s *Store) mayMarkFatal(err error) error {
	if errors.Is(err, ErrClosed) {
		return err
	}

	s.primaryMu.Lock()
	defer s.primaryMu.Unlock()

	if s.fatal != nil {
		return s.fatal
	}
	s.fatal = errors.Join(ErrFatal, err)
	return s.fatal
}

func cloneBytes(v []byte) []byte {
	return bytes.Clone(v)
}
