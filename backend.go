package minweight_store

import (
	"bytes"

	"github.com/JimChengLin/minpatricia"
)

type indexBackend struct {
	index             *minpatricia.Index
	records           indexRecordStore
	nodes             indexNodeStore
	verifyIndexOnRead bool
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
	Sync() error
	Close() error
	closeAfterSync() error
}

type indexNodeStore interface {
	minpatricia.NodeStore
	Sync() error
	Close() error
	closeAfterSync() error
}

type backendMutationResult uint8

const (
	backendMutationNotAccepted backendMutationResult = iota
	backendMutationApplied
	backendMutationAcceptedThenFailed
)

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

func (b *indexBackend) sync() error {
	var firstErr error
	if err := b.nodes.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := b.records.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
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

func (b *indexBackend) closeAfterSync() error {
	var firstErr error
	if err := b.nodes.closeAfterSync(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := b.records.closeAfterSync(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (b *indexBackend) put(key, value []byte) (backendMutationResult, error) {
	if len(key) > minpatricia.MaxKeySize {
		return backendMutationNotAccepted, minpatricia.ErrKeyTooLarge
	}
	pos, err := b.records.Append(key, value)
	if err != nil {
		return backendMutationNotAccepted, err
	}
	recordKey, ok := b.records.Key(pos)
	if !ok {
		return backendMutationAcceptedThenFailed, ErrCorruptIndex
	}

	old, replaced, err := b.index.Put(recordKey, pos)
	if err != nil {
		_ = b.records.Free(pos)
		return backendMutationAcceptedThenFailed, err
	}
	if replaced {
		if err := b.records.Free(old); err != nil {
			return backendMutationAcceptedThenFailed, err
		}
	}
	return backendMutationApplied, nil
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
	if err := b.verifyReadPosition(key, pos); err != nil {
		return nil, false, err
	}
	return cloneBytes(value), true, nil
}

func (b *indexBackend) delete(key []byte) (bool, backendMutationResult, error) {
	if len(key) > minpatricia.MaxKeySize {
		return false, backendMutationNotAccepted, minpatricia.ErrKeyTooLarge
	}
	pos, ok, err := b.index.Get(key)
	if err != nil {
		return false, backendMutationAcceptedThenFailed, err
	}
	if !ok {
		return false, backendMutationApplied, nil
	}
	if _, err := b.records.Delete(key); err != nil {
		return false, backendMutationNotAccepted, err
	}
	deletedPos, deleted, err := b.index.Delete(key)
	if err != nil {
		return deleted, backendMutationAcceptedThenFailed, err
	}
	if !deleted || deletedPos != pos {
		return false, backendMutationAcceptedThenFailed, ErrCorruptIndex
	}
	if err := b.records.Free(pos); err != nil {
		return true, backendMutationAcceptedThenFailed, err
	}
	return true, backendMutationApplied, nil
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
	if err := b.verifyReadPosition(key, pos); err != nil {
		return Item{}, err
	}
	return Item{
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	}, nil
}

func (b *indexBackend) verifyReadPosition(key []byte, pos minpatricia.Position) error {
	if !b.verifyIndexOnRead {
		return nil
	}
	got, ok, err := b.index.Get(key)
	if err != nil {
		return err
	}
	if !ok || got != pos {
		return ErrCorruptIndex
	}
	return nil
}
