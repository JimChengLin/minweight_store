package minweight_store

import "testing"

type testCloser interface {
	Close() error
}

func closeForTest(tb testing.TB, closer testCloser) {
	tb.Helper()

	if err := closer.Close(); err != nil {
		tb.Fatal(err)
	}
}

func stopCompactionDispatchersForTest(store *Store) {
	store.stopMinorCompactionDispatcher()
	store.stopMajorCompactionDispatcher()
}

func (b *indexBackend) syncAndClose() error {
	firstErr := b.sync()
	var err error
	if firstErr == nil {
		err = b.closeAfterSync()
	} else {
		err = b.close()
	}
	if err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
