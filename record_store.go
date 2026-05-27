package minweight

import "github.com/JimChengLin/minpatricia"

type record struct {
	key   []byte
	value []byte
}

type recordStore struct {
	records []record
	free    []minpatricia.Position
	live    int
}

func newRecordStore() *recordStore {
	return &recordStore{
		records: make([]record, 1),
	}
}

func (s *recordStore) Add(key, value []byte) minpatricia.Position {
	rec := record{
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
		return pos
	}

	pos := minpatricia.Position(len(s.records))
	s.records = append(s.records, rec)
	s.live++
	return pos
}

func (s *recordStore) Free(pos minpatricia.Position) error {
	if pos == 0 || uint64(pos) >= uint64(len(s.records)) || s.records[pos].key == nil {
		return ErrCorruptIndex
	}
	s.records[pos] = record{}
	s.free = append(s.free, pos)
	s.live--
	return nil
}

func (s *recordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	if pos == 0 || uint64(pos) >= uint64(len(s.records)) {
		return nil, false
	}
	rec := s.records[pos]
	if rec.key == nil {
		return nil, false
	}
	return rec.key, true
}

func (s *recordStore) Value(pos minpatricia.Position) ([]byte, bool) {
	if pos == 0 || uint64(pos) >= uint64(len(s.records)) {
		return nil, false
	}
	rec := s.records[pos]
	if rec.key == nil {
		return nil, false
	}
	return rec.value, true
}

func (s *recordStore) Len() int {
	return s.live
}

func cloneRecordKey(key []byte) []byte {
	if key == nil {
		return []byte{}
	}
	return cloneBytes(key)
}
