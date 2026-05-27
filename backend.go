package minweight_store

import (
	"bytes"

	"github.com/JimChengLin/minpatricia"
)

type indexBackend struct {
	index   *minpatricia.Index
	records indexRecordStore
	nodes   indexNodeStore
}

// indexRecordStore is the record backend API that indexBackend needs above
// minpatricia.RecordStore: appends allocate positions, deletes can append
// tombstones, and reads resolve positions back to values.
type indexRecordStore interface {
	minpatricia.RecordStore
	Append(key, value []byte) (minpatricia.Position, error)
	Delete(key []byte) (minpatricia.Position, error)
	Free(pos minpatricia.Position) error
	Value(pos minpatricia.Position) ([]byte, bool)
	Len() int
	Close() error
}

type indexNodeStore interface {
	minpatricia.NodeStore
	Close() error
}

func newIndexBackend() *indexBackend {
	records := newHeapRecordStore()
	nodes := newHeapNodeStore()
	return newIndexBackendWithNodes(records, nodes)
}

func newIndexBackendWithNodes(records indexRecordStore, nodes indexNodeStore) *indexBackend {
	return &indexBackend{
		index:   minpatricia.NewWithNodes(records, nodes),
		records: records,
		nodes:   nodes,
	}
}

func openIndexBackend(records indexRecordStore, nodes indexNodeStore) (*indexBackend, error) {
	index, err := minpatricia.OpenWithNodes(records, nodes)
	if err != nil {
		return nil, err
	}
	return &indexBackend{
		index:   index,
		records: records,
		nodes:   nodes,
	}, nil
}

func (b *indexBackend) len() int {
	return b.index.Len()
}

func (b *indexBackend) close() error {
	var firstErr error
	if err := b.nodes.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := b.records.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (b *indexBackend) put(key, value []byte) error {
	pos, err := b.records.Append(key, value)
	if err != nil {
		return err
	}
	recordKey, ok := b.records.Key(pos)
	if !ok {
		return ErrCorruptIndex
	}

	old, replaced, err := b.index.Put(recordKey, pos)
	if err != nil {
		_ = b.records.Free(pos)
		return err
	}
	if replaced {
		if err := b.records.Free(old); err != nil {
			return err
		}
	}
	return nil
}

func (b *indexBackend) get(key []byte) ([]byte, bool, error) {
	pos, ok, err := b.index.Get(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	value, ok := b.records.Value(pos)
	if !ok {
		return nil, false, ErrCorruptIndex
	}
	return cloneBytes(value), true, nil
}

func (b *indexBackend) delete(key []byte) (bool, error) {
	if _, err := b.records.Delete(key); err != nil {
		return false, err
	}
	pos, deleted, err := b.index.Delete(key)
	if err != nil || !deleted {
		return deleted, err
	}
	if err := b.records.Free(pos); err != nil {
		return true, err
	}
	return true, nil
}

func (b *indexBackend) scan(fn VisitFunc) error {
	var visitErr error
	err := b.index.Ascend(func(key []byte, pos minpatricia.Position) bool {
		item, err := b.item(key, pos)
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

func (b *indexBackend) scanRange(greaterOrEqual, lessThan []byte, fn VisitFunc) error {
	if lessThan != nil && bytes.Compare(greaterOrEqual, lessThan) > 0 {
		return ErrInvalidRange
	}

	var visitErr error
	visit := func(key []byte, pos minpatricia.Position) bool {
		item, err := b.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		return fn(item)
	}

	var err error
	if lessThan == nil {
		err = b.index.AscendGreaterOrEqual(greaterOrEqual, visit)
	} else {
		err = b.index.AscendRange(greaterOrEqual, lessThan, visit)
	}
	if err != nil {
		return err
	}
	return visitErr
}

func (b *indexBackend) reverseScan(fn VisitFunc) error {
	var visitErr error
	err := b.index.Descend(func(key []byte, pos minpatricia.Position) bool {
		item, err := b.item(key, pos)
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

func (b *indexBackend) reverseScanRange(lessOrEqual, greaterThan []byte, fn VisitFunc) error {
	if greaterThan != nil && bytes.Compare(greaterThan, lessOrEqual) > 0 {
		return ErrInvalidRange
	}

	var visitErr error
	visit := func(key []byte, pos minpatricia.Position) bool {
		item, err := b.item(key, pos)
		if err != nil {
			visitErr = err
			return false
		}
		return fn(item)
	}

	var err error
	if greaterThan == nil {
		err = b.index.DescendLessOrEqual(lessOrEqual, visit)
	} else {
		err = b.index.DescendRange(lessOrEqual, greaterThan, visit)
	}
	if err != nil {
		return err
	}
	return visitErr
}

func (b *indexBackend) seekGE(key []byte) (Item, bool, error) {
	var item Item
	var found bool
	var visitErr error
	err := b.index.AscendGreaterOrEqual(key, func(key []byte, pos minpatricia.Position) bool {
		var err error
		item, err = b.item(key, pos)
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

func (b *indexBackend) seekLE(key []byte) (Item, bool, error) {
	var item Item
	var found bool
	var visitErr error
	err := b.index.DescendLessOrEqual(key, func(key []byte, pos minpatricia.Position) bool {
		var err error
		item, err = b.item(key, pos)
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

func (b *indexBackend) item(key []byte, pos minpatricia.Position) (Item, error) {
	value, ok := b.records.Value(pos)
	if !ok {
		return Item{}, ErrCorruptIndex
	}
	return Item{
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	}, nil
}
