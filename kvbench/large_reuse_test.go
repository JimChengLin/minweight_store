package kvbench

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLargeLoadStoreDir(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "BenchmarkLargeLoad_minweight-minweight-123")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := largeLoadStoreDir(root, "minweight")
	if err != nil {
		t.Fatal(err)
	}
	if got != dir {
		t.Fatalf("largeLoadStoreDir = %q, want %q", got, dir)
	}
}

func TestLargeLoadStoreDirRequiresSingleMatch(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{
		"BenchmarkLargeLoad_minweight-minweight-1",
		"BenchmarkLargeLoad_minweight-minweight-2",
	} {
		if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := largeLoadStoreDir(root, "minweight"); err == nil {
		t.Fatal("largeLoadStoreDir succeeded with multiple matches")
	}
}
