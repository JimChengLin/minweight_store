package minweight

import (
	"bytes"
	"errors"
	"sync"

	"github.com/JimChengLin/minpatricia"
)

var (
	ErrInvalidRange = errors.New("minweight_store: invalid range")
	ErrCorruptIndex = errors.New("minweight_store: index points to missing record")
)

type Store struct {
	mu      sync.RWMutex
	index   *minpatricia.Index
	records *minpatricia.HeapRecordStore[[]byte]
}

type Item struct {
	Key   []byte
	Value []byte
}

type VisitFunc func(Item) bool

func New() *Store {
	index, records := minpatricia.NewHeap[[]byte]()
	return &Store{
		index:   index,
		records: records,
	}
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Len()
}

func (s *Store) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyCopy := cloneBytes(key)
	valueCopy := cloneBytes(value)
	pos := s.records.Add(keyCopy, valueCopy)

	old, replaced, err := s.index.Put(keyCopy, pos)
	if err != nil {
		_ = s.records.Free(pos)
		return err
	}
	if replaced {
		if err := s.records.Free(old); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pos, ok, err := s.index.Get(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	value, ok := s.records.Value(pos)
	if !ok {
		return nil, false, ErrCorruptIndex
	}
	return cloneBytes(value), true, nil
}

func (s *Store) Delete(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos, deleted, err := s.index.Delete(key)
	if err != nil || !deleted {
		return deleted, err
	}
	if err := s.records.Free(pos); err != nil {
		return true, err
	}
	return true, nil
}

func (s *Store) Scan(fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var visitErr error
	err := s.index.Ascend(func(key []byte, pos minpatricia.Position) bool {
		item, err := s.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		return fn(item)
	})
	if err != nil {
		return err
	}
	return visitErr
}

func (s *Store) ScanRange(greaterOrEqual, lessThan []byte, fn VisitFunc) error {
	if lessThan != nil && bytes.Compare(greaterOrEqual, lessThan) > 0 {
		return ErrInvalidRange
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var visitErr error
	visit := func(key []byte, pos minpatricia.Position) bool {
		item, err := s.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		return fn(item)
	}

	var err error
	if lessThan == nil {
		err = s.index.AscendGreaterOrEqual(greaterOrEqual, visit)
	} else {
		err = s.index.AscendRange(greaterOrEqual, lessThan, visit)
	}
	if err != nil {
		return err
	}
	return visitErr
}

func (s *Store) ReverseScan(fn VisitFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var visitErr error
	err := s.index.Descend(func(key []byte, pos minpatricia.Position) bool {
		item, err := s.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		return fn(item)
	})
	if err != nil {
		return err
	}
	return visitErr
}

func (s *Store) ReverseScanRange(lessOrEqual, greaterThan []byte, fn VisitFunc) error {
	if greaterThan != nil && bytes.Compare(greaterThan, lessOrEqual) > 0 {
		return ErrInvalidRange
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var visitErr error
	visit := func(key []byte, pos minpatricia.Position) bool {
		item, err := s.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		return fn(item)
	}

	var err error
	if greaterThan == nil {
		err = s.index.DescendLessOrEqual(lessOrEqual, visit)
	} else {
		err = s.index.DescendRange(lessOrEqual, greaterThan, visit)
	}
	if err != nil {
		return err
	}
	return visitErr
}

func (s *Store) SeekGE(key []byte) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var item Item
	var found bool
	var visitErr error
	err := s.index.AscendGreaterOrEqual(key, func(key []byte, pos minpatricia.Position) bool {
		var err error
		item, err = s.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		found = true
		return false
	})
	if visitErr != nil {
		return Item{}, false, visitErr
	}
	if err != nil || !found {
		return Item{}, found, err
	}
	return item, true, nil
}

func (s *Store) SeekLE(key []byte) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var item Item
	var found bool
	var visitErr error
	err := s.index.DescendLessOrEqual(key, func(key []byte, pos minpatricia.Position) bool {
		var err error
		item, err = s.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		found = true
		return false
	})
	if visitErr != nil {
		return Item{}, false, visitErr
	}
	if err != nil || !found {
		return Item{}, found, err
	}
	return item, true, nil
}

func (s *Store) item(key []byte, pos minpatricia.Position) (Item, error) {
	value, ok := s.records.Value(pos)
	if !ok {
		return Item{}, ErrCorruptIndex
	}
	return Item{
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	}, nil
}

func cloneBytes(v []byte) []byte {
	return bytes.Clone(v)
}
