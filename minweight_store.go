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
	ErrCorruptWAL   = errors.New("minweight_store: corrupt wal")
	ErrClosed       = errors.New("minweight_store: store is closed")
	ErrFatal        = errors.New("minweight_store: store is fatal")
	ErrReplayPolicy = errors.New("minweight_store: invalid wal replay policy")
	ErrManifest     = errors.New("minweight_store: corrupt manifest")
	ErrParquet      = errors.New("minweight_store: invalid parquet record store")
)

type Store struct {
	mu       sync.RWMutex
	backend  *indexBackend
	manifest *manifest
	wal      *mmapWALRecordStore
	fatal    error
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.backend == nil || s.fatal != nil {
		return 0
	}
	return s.backend.len()
}

func (s *Store) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	result, err := backend.put(key, value)
	if err != nil && result == backendMutationAcceptedThenFailed {
		return s.markFatal(err)
	}
	return err
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return nil, false, err
	}
	return backend.get(key)
}

func (s *Store) Delete(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	backend, err := s.openBackend()
	if err != nil {
		return false, err
	}
	deleted, result, err := backend.delete(key)
	if err != nil && result == backendMutationAcceptedThenFailed {
		return deleted, s.markFatal(err)
	}
	return deleted, err
}

func (s *Store) Scan(fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.scan(fn)
}

func (s *Store) ScanRange(greaterOrEqual, lessThan []byte, fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.scanRange(greaterOrEqual, lessThan, fn)
}

func (s *Store) ReverseScan(fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.reverseScan(fn)
}

func (s *Store) ReverseScanRange(lessOrEqual, greaterThan []byte, fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	return backend.reverseScanRange(lessOrEqual, greaterThan, fn)
}

func (s *Store) SeekGE(key []byte) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return Item{}, false, err
	}
	return backend.seekGE(key)
}

func (s *Store) SeekLE(key []byte) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return Item{}, false, err
	}
	return backend.seekLE(key)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.backend == nil {
		return nil
	}
	backend := s.backend
	fatal := s.fatal
	manifest := s.manifest
	wal := s.wal
	s.backend = nil
	s.manifest = nil
	s.wal = nil

	var firstErr error
	if fatal == nil && manifest != nil && wal != nil {
		if err := backend.sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if firstErr == nil {
			if err := manifest.write(wal.used); err != nil {
				firstErr = err
			}
		}
	}
	if err := backend.close(); err != nil && firstErr == nil {
		firstErr = err
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

func (s *Store) markFatal(err error) error {
	s.fatal = errors.Join(ErrFatal, err)
	return s.fatal
}

func cloneBytes(v []byte) []byte {
	return bytes.Clone(v)
}
