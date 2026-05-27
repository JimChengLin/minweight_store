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
)

type Store struct {
	mu      sync.RWMutex
	backend *indexBackend
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

	if s.backend == nil {
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
	return backend.put(key, value)
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
	return backend.delete(key)
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
	s.backend = nil
	return backend.close()
}

func (s *Store) openBackend() (*indexBackend, error) {
	if s.backend == nil {
		return nil, ErrClosed
	}
	return s.backend, nil
}

func cloneBytes(v []byte) []byte {
	return bytes.Clone(v)
}
