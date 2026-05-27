//go:build darwin || linux

package minweight

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

type nodeStoreBenchSize struct {
	name string
	n    int
}

type nodeStoreBenchData struct {
	keys      [][]byte
	records   *recordStore
	positions []minpatricia.Position
}

var nodeStoreBenchSizes = []nodeStoreBenchSize{
	{name: "1K", n: 1_000},
	{name: "10K", n: 10_000},
}

func BenchmarkNodeStoreGet(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newNodeStoreBenchData(size.n)
		benchmarkNodeStores(b, size.name, data, func(b *testing.B, idx *minpatricia.Index) {
			b.ReportAllocs()
			b.ResetTimer()

			var sink minpatricia.Position
			for i := 0; i < b.N; i++ {
				pos, ok, err := idx.Get(data.keys[i%len(data.keys)])
				if err != nil || !ok {
					b.Fatalf("Get failed: pos=%d ok=%v err=%v", pos, ok, err)
				}
				sink = pos
			}
			_ = sink
		})
	}
}

func BenchmarkNodeStoreSeekGE(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newNodeStoreBenchData(size.n)
		benchmarkNodeStores(b, size.name, data, func(b *testing.B, idx *minpatricia.Index) {
			b.ReportAllocs()
			b.ResetTimer()

			var sink minpatricia.Position
			for i := 0; i < b.N; i++ {
				found := false
				err := idx.AscendGreaterOrEqual(data.keys[i%len(data.keys)], func(_ []byte, pos minpatricia.Position) bool {
					sink = pos
					found = true
					return false
				})
				if err != nil || !found {
					b.Fatalf("SeekGE failed: found=%v err=%v", found, err)
				}
			}
			_ = sink
		})
	}
}

func BenchmarkNodeStoreScan(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newNodeStoreBenchData(size.n)
		benchmarkNodeStores(b, size.name, data, func(b *testing.B, idx *minpatricia.Index) {
			b.ReportAllocs()
			b.ResetTimer()

			var sink minpatricia.Position
			for i := 0; i < b.N; i++ {
				err := idx.Ascend(func(_ []byte, pos minpatricia.Position) bool {
					sink = pos
					return true
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			_ = sink
		})
	}
}

func BenchmarkNodeStorePutReplace(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newNodeStoreBenchData(size.n)
		replacements := newNodeStoreBenchReplacementPositions(data)
		benchmarkNodeStores(b, size.name, data, func(b *testing.B, idx *minpatricia.Index) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				j := i % len(data.keys)
				if _, _, err := idx.Put(data.keys[j], replacements[j]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkNodeStores(b *testing.B, sizeName string, data nodeStoreBenchData, run func(*testing.B, *minpatricia.Index)) {
	b.Helper()

	b.Run(sizeName+"/original_heap", func(b *testing.B) {
		idx := buildNodeStoreBenchIndex(b, data, minpatricia.NewHeapNodeStore())
		run(b, idx)
	})
	b.Run(sizeName+"/mmap", func(b *testing.B) {
		nodes, err := openMmapNodeStore(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := nodes.Close(); err != nil {
				b.Fatal(err)
			}
		})
		idx := buildNodeStoreBenchIndex(b, data, nodes)
		run(b, idx)
	})
}

func newNodeStoreBenchData(n int) nodeStoreBenchData {
	rng := rand.New(rand.NewSource(int64(n)))
	records := newRecordStore()
	keys := make([][]byte, 0, n)
	positions := make([]minpatricia.Position, 0, n)
	seen := make(map[string]struct{}, n)

	for len(keys) < n {
		key := fmt.Sprintf("key-%08x-%08x", rng.Uint32(), rng.Uint32())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		keyBytes := []byte(key)
		pos := records.Add(keyBytes, nil)
		keys = append(keys, keyBytes)
		positions = append(positions, pos)
	}

	return nodeStoreBenchData{
		keys:      keys,
		records:   records,
		positions: positions,
	}
}

func buildNodeStoreBenchIndex(tb testing.TB, data nodeStoreBenchData, nodes minpatricia.NodeStore) *minpatricia.Index {
	tb.Helper()

	idx := minpatricia.NewWithNodes(data.records, nodes)
	for i, key := range data.keys {
		if _, _, err := idx.Put(key, data.positions[i]); err != nil {
			tb.Fatal(err)
		}
	}
	return idx
}

func newNodeStoreBenchReplacementPositions(data nodeStoreBenchData) []minpatricia.Position {
	replacements := make([]minpatricia.Position, len(data.keys))
	for i, key := range data.keys {
		replacements[i] = data.records.Add(key, nil)
	}
	return replacements
}
