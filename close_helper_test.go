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
