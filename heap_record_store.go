package minweight_store

import "github.com/JimChengLin/minpatricia"

type heapRecord struct {
	key   []byte
	value []byte
}

type heapRecordStore struct {
	records []heapRecord
	free    []minpatricia.Position
	live    int
}

func newHeapRecordStore() *heapRecordStore {
	return &heapRecordStore{
		records: make([]heapRecord, 1),
	}
}

func (s *heapRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	rec := heapRecord{
		key:   cloneRecordKey(key),
		value: cloneBytes(value),
	}
	if len(s.free) != 0 {
		last := len(s.free) - 1
		pos := s.free[last]
		s.free[last] = 0
		s.free = s.free[:last]
		s.records[pos] = rec
		s.live++
		return pos, nil
	}

	pos := minpatricia.Position(len(s.records))
	s.records = append(s.records, rec)
	s.live++
	return pos, nil
}

func (s *heapRecordStore) Delete(key []byte) (minpatricia.Position, error) {
	return 0, nil
}

func (s *heapRecordStore) Free(pos minpatricia.Position) error {
	if pos == 0 || uint64(pos) >= uint64(len(s.records)) || s.records[pos].key == nil {
		return ErrCorruptIndex
	}
	s.records[pos] = heapRecord{}
	s.free = append(s.free, pos)
	s.live--
	return nil
}

func (s *heapRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	if pos == 0 || uint64(pos) >= uint64(len(s.records)) {
		return nil, false
	}
	rec := s.records[pos]
	if rec.key == nil {
		return nil, false
	}
	return rec.key, true
}

func (s *heapRecordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	if pos == 0 || uint64(pos) >= uint64(len(s.records)) {
		return nil, false
	}
	rec := s.records[pos]
	if rec.key == nil {
		return nil, false
	}
	return rec.value, true
}

func (s *heapRecordStore) Len() int {
	return s.live
}

func (s *heapRecordStore) Sync() error {
	return nil
}

func (s *heapRecordStore) Close() error {
	return nil
}

func cloneRecordKey(key []byte) []byte {
	if key == nil {
		return []byte{}
	}
	return cloneBytes(key)
}
