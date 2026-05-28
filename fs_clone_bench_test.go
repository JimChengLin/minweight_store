//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkMmapNodeExtentCopyMethod(b *testing.B) {
	for _, liveNodes := range []int{128, 1024} {
		b.Run(fmt.Sprintf("%d_live_nodes", liveNodes), func(b *testing.B) {
			b.Run("fs_clone", func(b *testing.B) {
				src := prepareMmapNodeExtentCopyBench(b, liveNodes)
				root := b.TempDir()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst := filepath.Join(root, fmt.Sprintf("clone-%06d.nodes", i))
					if err := copyMmapNodeExtentFile(src, dst); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("sparse_copy", func(b *testing.B) {
				src := prepareMmapNodeExtentCopyBench(b, liveNodes)
				root := b.TempDir()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst := filepath.Join(root, fmt.Sprintf("sparse-%06d.nodes", i))
					if err := copyMmapNodeExtentFileSparse(src, dst); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func prepareMmapNodeExtentCopyBench(tb testing.TB, liveNodes int) string {
	tb.Helper()

	dir := tb.TempDir()
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < liveNodes; i++ {
		_, page, err := nodes.Alloc()
		if err != nil {
			tb.Fatal(err)
		}
		copy(mmapNodePageBytes(page), []byte("copy-bench-node"))
	}
	if err := nodes.Sync(); err != nil {
		tb.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		tb.Fatal(err)
	}
	return filepath.Join(dir, mmapNodeExtentName(0))
}
