//go:build darwin || linux

package minweight_store

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
	records   *heapRecordStore
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

func BenchmarkMmapNodeStoreAllocAfterFullExtents(b *testing.B) {
	for _, fullExtents := range []int{1, 8} {
		b.Run(fmt.Sprintf("full_extents_%d/active_extent", fullExtents), func(b *testing.B) {
			benchmarkMmapNodeStoreAllocAfterFullExtents(b, fullExtents, (*mmapNodeStore).Alloc)
		})
		b.Run(fmt.Sprintf("full_extents_%d/linear_scan_baseline", fullExtents), func(b *testing.B) {
			benchmarkMmapNodeStoreAllocAfterFullExtents(b, fullExtents, allocMmapNodeStoreByLinearScanForBench)
		})
	}
}

func BenchmarkMmapNodeStoreAllocAfterManyFullExtents(b *testing.B) {
	for _, fullExtents := range []int{64, 1024} {
		b.Run(fmt.Sprintf("full_extents_%d/active_extent", fullExtents), func(b *testing.B) {
			benchmarkFakeMmapNodeStoreAllocAfterFullExtents(b, fullExtents, (*mmapNodeStore).Alloc)
		})
		b.Run(fmt.Sprintf("full_extents_%d/linear_scan_baseline", fullExtents), func(b *testing.B) {
			benchmarkFakeMmapNodeStoreAllocAfterFullExtents(b, fullExtents, allocMmapNodeStoreByLinearScanForBench)
		})
	}
}

func benchmarkMmapNodeStoreAllocAfterFullExtents(
	b *testing.B,
	fullExtents int,
	alloc func(*mmapNodeStore) (uint64, *minpatricia.NodePage, error),
) {
	b.Helper()

	nodes := newMmapNodeStoreAllocBench(b, fullExtents)
	extent := nodes.extents[fullExtents]
	wantID := uint64(fullExtents) * mmapNodeSlotsPerExtent

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _, err := alloc(nodes)
		if err != nil {
			b.Fatal(err)
		}
		if id != wantID {
			b.Fatalf("Alloc id = %d, want %d", id, wantID)
		}
		extent.setUsed(0, false)
		extent.setLiveSlots(0)
		nodes.setPage(id, nil)
	}
}

func benchmarkFakeMmapNodeStoreAllocAfterFullExtents(
	b *testing.B,
	fullExtents int,
	alloc func(*mmapNodeStore) (uint64, *minpatricia.NodePage, error),
) {
	b.Helper()

	nodes := newFakeMmapNodeStoreAllocBench(fullExtents)
	extent := nodes.extents[fullExtents]
	wantID := uint64(fullExtents) * mmapNodeSlotsPerExtent

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _, err := alloc(nodes)
		if err != nil {
			b.Fatal(err)
		}
		if id != wantID {
			b.Fatalf("Alloc id = %d, want %d", id, wantID)
		}
		extent.setUsed(0, false)
		extent.setLiveSlots(0)
		nodes.setPage(id, nil)
	}
}

func allocMmapNodeStoreByLinearScanForBench(s *mmapNodeStore) (uint64, *minpatricia.NodePage, error) {
	for _, extent := range s.extents {
		if extent == nil || extent.liveSlots() == mmapNodeSlotsPerExtent {
			continue
		}
		id, page, ok := extent.alloc()
		if !ok {
			continue
		}
		if id&minpatriciaHandleTag != 0 {
			return 0, nil, minpatricia.ErrPositionTag
		}
		s.setPage(id, page)
		return id, page, nil
	}
	s.activeExtentIndex = len(s.extents)
	return s.Alloc()
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
	records := newHeapRecordStore()
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
		pos, err := records.Append(keyBytes, nil)
		if err != nil {
			panic(err)
		}
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
		pos, err := data.records.Append(key, nil)
		if err != nil {
			panic(err)
		}
		replacements[i] = pos
	}
	return replacements
}

func newMmapNodeStoreAllocBench(b *testing.B, fullExtents int) *mmapNodeStore {
	b.Helper()

	dir := b.TempDir()
	extents := make([]*mmapNodeExtent, fullExtents+1)
	for id := range extents {
		extent, err := createMmapNodeExtent(dir, uint64(id))
		if err != nil {
			closeMmapNodeExtents(extents)
			b.Fatal(err)
		}
		if id < fullExtents {
			markMmapNodeExtentFullForBench(extent)
		}
		extents[id] = extent
	}
	nodes := &mmapNodeStore{
		dir:     dir,
		extents: extents,
	}
	nodes.ensurePageSlots(uint64(fullExtents))
	nodes.activeExtentIndex = nodes.firstAllocExtentIndex(0)
	b.Cleanup(func() {
		if err := nodes.Close(); err != nil {
			b.Fatal(err)
		}
	})
	return nodes
}

func newFakeMmapNodeStoreAllocBench(fullExtents int) *mmapNodeStore {
	extents := make([]*mmapNodeExtent, fullExtents+1)
	for id := range extents {
		extent := &mmapNodeExtent{
			id:   uint64(id),
			data: make([]byte, mmapNodePageSize*3),
		}
		if id < fullExtents {
			extent.setLiveSlots(mmapNodeSlotsPerExtent)
		}
		extents[id] = extent
	}
	nodes := &mmapNodeStore{
		extents:           extents,
		activeExtentIndex: fullExtents,
	}
	nodes.ensurePageSlots(uint64(fullExtents))
	return nodes
}

func markMmapNodeExtentFullForBench(extent *mmapNodeExtent) {
	bitmap := extent.bitmap()
	for byteIndex := range bitmap {
		bitmap[byteIndex] = bitsetByteMask(byteIndex, mmapNodeSlotsPerExtent)
	}
	extent.setLiveSlots(mmapNodeSlotsPerExtent)
}
