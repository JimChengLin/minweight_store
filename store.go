package minweight

import (
	"bytes"
	"errors"
	"sync"
)

var (
	ErrInvalidRange = errors.New("minweight_store: invalid range")
	ErrCorruptIndex = errors.New("minweight_store: index points to missing record")
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

	return s.backend.len()
}

func (s *Store) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.backend.put(key, value)
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.get(key)
}

func (s *Store) Delete(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.backend.delete(key)
}

func (s *Store) Scan(fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.scan(fn)
}

func (s *Store) ScanRange(greaterOrEqual, lessThan []byte, fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.scanRange(greaterOrEqual, lessThan, fn)
}

func (s *Store) ReverseScan(fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.reverseScan(fn)
}

func (s *Store) ReverseScanRange(lessOrEqual, greaterThan []byte, fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.reverseScanRange(lessOrEqual, greaterThan, fn)
}

func (s *Store) SeekGE(key []byte) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.seekGE(key)
}

func (s *Store) SeekLE(key []byte) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backend.seekLE(key)
}

func cloneBytes(v []byte) []byte {
	return bytes.Clone(v)
}
