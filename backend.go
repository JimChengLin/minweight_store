package minweight

import (
	"bytes"

	"github.com/JimChengLin/minpatricia"
)

type indexBackend struct {
	index   *minpatricia.Index
	records *recordStore
	nodes   minpatricia.NodeStore
}

func newIndexBackend() *indexBackend {
	records := newRecordStore()
	nodes := newHeapNodeStore()
	return newIndexBackendWithNodes(records, nodes)
}

func newIndexBackendWithNodes(records *recordStore, nodes minpatricia.NodeStore) *indexBackend {
	return &indexBackend{
		index:   minpatricia.NewWithNodes(records, nodes),
		records: records,
		nodes:   nodes,
	}
}

func openIndexBackend(records *recordStore, nodes minpatricia.NodeStore) (*indexBackend, error) {
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

func (b *indexBackend) put(key, value []byte) error {
	pos := b.records.Add(key, value)
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
