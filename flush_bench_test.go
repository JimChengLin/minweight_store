//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

const (
	flushBenchRecordsPerCycle = 64
	flushBenchTailRecords     = 32
	flushBenchKeyLen          = 24
	flushBenchValueLen        = 128
)

func BenchmarkStoreFlushPut(b *testing.B) {
	for _, records := range []int{64, 1024} {
		b.Run(fmt.Sprintf("%d_records", records), func(b *testing.B) {
			benchmarkStoreFlushPut(b, records)
		})
	}
}

func benchmarkStoreFlushPut(b *testing.B, recordsPerCycle int) {
	value := bytes.Repeat([]byte{'v'}, flushBenchValueLen)
	walSize := flushBenchWALSize(recordsPerCycle, flushBenchValueLen)
	store, err := Open(b.TempDir(), Options{WALSize: walSize})
	if err != nil {
		b.Fatal(err)
	}
	if err := store.Put(flushBenchKey('p', 0), value); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(flushBenchBytes(recordsPerCycle, flushBenchValueLen))
	b.ResetTimer()

	nextKey := 1
	for i := 0; i < b.N; i++ {
		for j := 0; j < recordsPerCycle; j++ {
			if err := store.Put(flushBenchKey('p', nextKey), value); err != nil {
				b.Fatal(err)
			}
			nextKey++
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(recordsPerCycle), "records/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(recordsPerCycle)), "ns/record")
	closeForTest(b, store)
}

func BenchmarkStoreFlushDeleteLog(b *testing.B) {
	for _, records := range []int{64, 1024} {
		b.Run(fmt.Sprintf("%d_records", records), func(b *testing.B) {
			benchmarkStoreFlushDeleteLog(b, records)
		})
	}
}

func benchmarkStoreFlushDeleteLog(b *testing.B, recordsPerCycle int) {
	value := bytes.Repeat([]byte{'v'}, 1)
	walSize := flushBenchWALSize(recordsPerCycle, 0)
	store, err := Open(b.TempDir(), Options{WALSize: walSize})
	if err != nil {
		b.Fatal(err)
	}
	totalKeys := recordsPerCycle*b.N + 1
	for i := 0; i < totalKeys; i++ {
		if err := store.Put(flushBenchKey('d', i), value); err != nil {
			b.Fatal(err)
		}
	}
	if err := store.flush(); err != nil {
		b.Fatal(err)
	}
	deleted, err := store.Delete(flushBenchKey('d', 0))
	if err != nil || !deleted {
		b.Fatalf("Delete(seed) = (%v,%v), want true,nil", deleted, err)
	}

	b.SetBytes(flushBenchBytes(recordsPerCycle, 0))
	b.ResetTimer()

	nextKey := 1
	for i := 0; i < b.N; i++ {
		for j := 0; j < recordsPerCycle; j++ {
			if _, err := store.Delete(flushBenchKey('d', nextKey)); err != nil {
				b.Fatal(err)
			}
			nextKey++
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(recordsPerCycle), "records/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(recordsPerCycle)), "ns/record")
	closeForTest(b, store)
}

func BenchmarkStoreGracefulShutdownFlush(b *testing.B) {
	for _, records := range []int{32, 512} {
		b.Run(fmt.Sprintf("%d_tail_records", records), func(b *testing.B) {
			benchmarkStoreGracefulShutdownFlush(b, records)
		})
	}
}

func benchmarkStoreGracefulShutdownFlush(b *testing.B, tailRecords int) {
	value := bytes.Repeat([]byte{'v'}, flushBenchValueLen)
	walSize := flushBenchWALSize(tailRecords+1, flushBenchValueLen)
	root := b.TempDir()

	b.SetBytes(flushBenchBytes(tailRecords, flushBenchValueLen))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("close-%06d", i))
		store := openFlushBenchStoreWithTail(b, dir, walSize, value, i, tailRecords)

		b.StartTimer()
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(tailRecords), "tail_records/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(tailRecords)), "ns/tail_record")
}

func BenchmarkStoreOpenTailReplay(b *testing.B) {
	for _, records := range []int{32, 512} {
		b.Run(fmt.Sprintf("%d_tail_records", records), func(b *testing.B) {
			benchmarkStoreOpenTailReplay(b, records)
		})
	}
}

func BenchmarkMmapNodeStoreCopyEqual(b *testing.B) {
	for _, liveNodes := range []int{128, 1024} {
		b.Run(fmt.Sprintf("%d_live_nodes", liveNodes), func(b *testing.B) {
			src, dst := prepareMmapNodeStoreCopyBench(b, liveNodes)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := copyMmapNodeStoreDir(src, dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMmapNodeStoreCopyOnePageDifferent(b *testing.B) {
	for _, liveNodes := range []int{128, 1024} {
		b.Run(fmt.Sprintf("%d_live_nodes", liveNodes), func(b *testing.B) {
			src, dst, ids := prepareMmapNodeStoreCopyBenchWithIDs(b, liveNodes)
			targetID := ids[len(ids)-1]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				writeMmapNodeStoreCopyBenchValue(b, src, targetID, uint64(i+1))
				b.StartTimer()
				if err := copyMmapNodeStoreDir(src, dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMmapNodeStoreCopyMissingDestination(b *testing.B) {
	for _, liveNodes := range []int{128, 1024} {
		b.Run(fmt.Sprintf("%d_live_nodes", liveNodes), func(b *testing.B) {
			src, _, _ := prepareMmapNodeStoreCopyBenchWithIDs(b, liveNodes)
			root := b.TempDir()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst := filepath.Join(root, fmt.Sprintf("dst-%06d", i))
				if err := copyMmapNodeStoreDir(src, dst); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := os.RemoveAll(dst); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkMmapNodeStoreCopyMatrix(b *testing.B) {
	for _, density := range mmapNodeStoreCopyBenchDensities() {
		b.Run(density.name, func(b *testing.B) {
			b.Run("all_equal", func(b *testing.B) {
				src, dst, _ := prepareMmapNodeStoreCopyBenchWithIDs(b, density.liveNodes)
				benchmarkMmapNodeStoreCopyExisting(b, src, dst, nil)
			})
			b.Run("one_page_diff", func(b *testing.B) {
				src, dst, ids := prepareMmapNodeStoreCopyBenchWithIDs(b, density.liveNodes)
				benchmarkMmapNodeStoreCopyExisting(b, src, dst, ids[len(ids)-1:])
			})
			b.Run("half_pages_diff", func(b *testing.B) {
				src, dst, ids := prepareMmapNodeStoreCopyBenchWithIDs(b, density.liveNodes)
				benchmarkMmapNodeStoreCopyExisting(b, src, dst, ids[:len(ids)/2])
			})
			b.Run("all_pages_diff", func(b *testing.B) {
				src, dst, ids := prepareMmapNodeStoreCopyBenchWithIDs(b, density.liveNodes)
				benchmarkMmapNodeStoreCopyExisting(b, src, dst, ids)
			})
			b.Run("missing_dst", func(b *testing.B) {
				src, _, _ := prepareMmapNodeStoreCopyBenchWithIDs(b, density.liveNodes)
				benchmarkMmapNodeStoreCopyMissing(b, src)
			})
		})
	}
}

func BenchmarkMmapNodeStoreCopyPartialFilesEqual(b *testing.B) {
	liveNodes := mmapNodeSlotsPerExtent + 128
	src, dst, ids := prepareMmapNodeStoreCopyBenchWithIDs(b, liveNodes)
	var secondExtentIDs []uint64
	for _, id := range ids {
		if id/mmapNodeSlotsPerExtent == 1 {
			secondExtentIDs = append(secondExtentIDs, id)
		}
	}
	benchmarkMmapNodeStoreCopyExisting(b, src, dst, secondExtentIDs)
}

func BenchmarkMmapNodeStoreCopyWorkers(b *testing.B) {
	for _, density := range []struct {
		name          string
		livePerExtent int
	}{
		{name: "sparse", livePerExtent: 128},
		{name: "dense", livePerExtent: mmapNodeSlotsPerExtent},
	} {
		b.Run(density.name, func(b *testing.B) {
			for _, workers := range []int{1, 2, 4} {
				b.Run(fmt.Sprintf("%d_workers_existing", workers), func(b *testing.B) {
					src, dst, ids := prepareMmapNodeStoreMultiExtentCopyBench(b, 4, density.livePerExtent)
					benchmarkMmapNodeStoreCopyExistingWithWorkers(b, src, dst, ids, workers)
				})
				b.Run(fmt.Sprintf("%d_workers_missing", workers), func(b *testing.B) {
					src, _, _ := prepareMmapNodeStoreMultiExtentCopyBench(b, 4, density.livePerExtent)
					benchmarkMmapNodeStoreCopyMissingWithWorkers(b, src, workers)
				})
			}
		})
	}
}

func BenchmarkMmapNodeStoreSyncWorkers(b *testing.B) {
	for _, extents := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("%d_extents", extents), func(b *testing.B) {
			for _, workers := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("%d_workers", workers), func(b *testing.B) {
					nodes := prepareMmapNodeStoreSyncBench(b, extents)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						dirtyMmapNodeStoreSyncBench(nodes, uint64(i+1))
						b.StartTimer()
						if err := nodes.syncWithWorkers(workers); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					closeForTest(b, nodes)
				})
			}
		})
	}
}

func BenchmarkMmapNodeExtentCreateDirSync(b *testing.B) {
	for _, extents := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("%d_extents", extents), func(b *testing.B) {
			benchmarkMmapNodeExtentCreateDirSync(b, extents)
		})
	}
}

func benchmarkMmapNodeExtentCreateDirSync(b *testing.B, extents int) {
	root := b.TempDir()
	b.ReportMetric(float64(extents), "extents/op")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("extents-%06d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatal(err)
		}
		created := make([]*mmapNodeExtent, 0, extents)
		b.StartTimer()

		for id := 0; id < extents; id++ {
			extent, err := createMmapNodeExtent(dir, uint64(id))
			if err != nil {
				b.Fatal(err)
			}
			created = append(created, extent)
		}
		if err := syncDir(dir); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		for _, extent := range created {
			if err := extent.closeAfterSync(); err != nil {
				b.Fatal(err)
			}
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(extents)), "ns/extent")
}

type mmapNodeStoreCopyBenchDensity struct {
	name      string
	liveNodes int
}

func mmapNodeStoreCopyBenchDensities() []mmapNodeStoreCopyBenchDensity {
	return []mmapNodeStoreCopyBenchDensity{
		{name: "sparse_128", liveNodes: 128},
		{name: "medium_1024", liveNodes: 1024},
		{name: "dense_full_extent", liveNodes: mmapNodeSlotsPerExtent - 1},
	}
}

func benchmarkMmapNodeStoreCopyExisting(b *testing.B, src, dst string, dirtyIDs []uint64) {
	benchmarkMmapNodeStoreCopyExistingWithWorkers(b, src, dst, dirtyIDs, mmapNodeStoreCopyWorkers)
}

func benchmarkMmapNodeStoreCopyExistingWithWorkers(b *testing.B, src, dst string, dirtyIDs []uint64, workers int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(dirtyIDs) != 0 {
			b.StopTimer()
			writeMmapNodeStoreCopyBenchValues(b, src, dirtyIDs, uint64(i+1))
			b.StartTimer()
		}
		if err := copyMmapNodeStoreDirWithWorkers(src, dst, workers); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMmapNodeStoreCopyMissing(b *testing.B, src string) {
	benchmarkMmapNodeStoreCopyMissingWithWorkers(b, src, mmapNodeStoreCopyWorkers)
}

func benchmarkMmapNodeStoreCopyMissingWithWorkers(b *testing.B, src string, workers int) {
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := filepath.Join(root, fmt.Sprintf("dst-%06d", i))
		if err := copyMmapNodeStoreDirWithWorkers(src, dst, workers); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := os.RemoveAll(dst); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

func prepareMmapNodeStoreMultiExtentCopyBench(tb testing.TB, extents, livePerExtent int) (string, string, []uint64) {
	tb.Helper()

	root := tb.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		tb.Fatal(err)
	}
	ids := make([]uint64, 0, extents*livePerExtent)
	for extentID := 0; extentID < extents; extentID++ {
		extent, err := createMmapNodeExtent(src, uint64(extentID))
		if err != nil {
			tb.Fatal(err)
		}
		for slot := uint64(0); slot < uint64(livePerExtent); slot++ {
			extent.setUsed(slot, true)
			id := uint64(extentID)*mmapNodeSlotsPerExtent + slot
			ids = append(ids, id)
			binary.LittleEndian.PutUint64(mmapNodePageBytes(extent.page(slot)), id)
		}
		extent.setLiveSlots(uint32(livePerExtent))
		if err := extent.close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := syncDir(src); err != nil {
		tb.Fatal(err)
	}
	if err := copyMmapNodeStoreDir(src, dst); err != nil {
		tb.Fatal(err)
	}
	return src, dst, ids
}

func prepareMmapNodeStoreSyncBench(tb testing.TB, extents int) *mmapNodeStore {
	tb.Helper()

	dir := tb.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatal(err)
	}
	for extentID := 0; extentID < extents; extentID++ {
		extent, err := createMmapNodeExtent(dir, uint64(extentID))
		if err != nil {
			tb.Fatal(err)
		}
		extent.setUsed(0, true)
		extent.setLiveSlots(1)
		binary.LittleEndian.PutUint64(mmapNodePageBytes(extent.page(0)), uint64(extentID))
		if err := extent.close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := syncDir(dir); err != nil {
		tb.Fatal(err)
	}
	nodes, err := openMmapNodeStore(dir)
	if err != nil {
		tb.Fatal(err)
	}
	return nodes
}

func dirtyMmapNodeStoreSyncBench(nodes *mmapNodeStore, value uint64) {
	for _, extent := range nodes.extents {
		if extent == nil {
			continue
		}
		binary.LittleEndian.PutUint64(mmapNodePageBytes(extent.page(0)), value)
	}
}

func benchmarkStoreOpenTailReplay(b *testing.B, tailRecords int) {
	value := bytes.Repeat([]byte{'v'}, flushBenchValueLen)
	walSize := flushBenchWALSize(tailRecords+1, flushBenchValueLen)
	root := b.TempDir()

	b.SetBytes(flushBenchBytes(tailRecords, flushBenchValueLen))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("open-%06d", i))
		store := openFlushBenchStoreWithTail(b, dir, walSize, value, i, tailRecords)
		dirtyCloseFlushBenchStore(b, store)

		b.StartTimer()
		recovered, err := Open(dir, Options{WALSize: walSize})
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		closeForTest(b, recovered)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(tailRecords), "tail_records/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(tailRecords)), "ns/tail_record")
}

func prepareMmapNodeStoreCopyBench(tb testing.TB, liveNodes int) (string, string) {
	src, dst, _ := prepareMmapNodeStoreCopyBenchWithIDs(tb, liveNodes)
	return src, dst
}

func prepareMmapNodeStoreCopyBenchWithIDs(tb testing.TB, liveNodes int) (string, string, []uint64) {
	tb.Helper()

	root := tb.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	nodes, err := openMmapNodeStore(src)
	if err != nil {
		tb.Fatal(err)
	}
	ids := make([]uint64, 0, liveNodes)
	for i := 0; i < liveNodes; i++ {
		id, page, err := nodes.Alloc()
		if err != nil {
			tb.Fatal(err)
		}
		ids = append(ids, id)
		binary.LittleEndian.PutUint64(mmapNodePageBytes(page), uint64(i))
	}
	if err := nodes.Sync(); err != nil {
		tb.Fatal(err)
	}
	if err := nodes.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := copyMmapNodeStoreDir(src, dst); err != nil {
		tb.Fatal(err)
	}
	return src, dst, ids
}

func writeMmapNodeStoreCopyBenchValue(tb testing.TB, dir string, id uint64, value uint64) {
	tb.Helper()

	writeMmapNodeStoreCopyBenchValues(tb, dir, []uint64{id}, value)
}

func writeMmapNodeStoreCopyBenchValues(tb testing.TB, dir string, ids []uint64, value uint64) {
	tb.Helper()

	byFileNo := make(map[uint64][]uint64)
	for _, id := range ids {
		fileNo := id / mmapNodeSlotsPerExtent
		byFileNo[fileNo] = append(byFileNo[fileNo], id)
	}
	for fileNo, ids := range byFileNo {
		writeMmapNodeStoreCopyBenchExtentValues(tb, dir, fileNo, ids, value)
	}
}

func writeMmapNodeStoreCopyBenchExtentValues(tb testing.TB, dir string, fileNo uint64, ids []uint64, value uint64) {
	tb.Helper()

	path := filepath.Join(dir, mmapNodeExtentName(fileNo))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		tb.Fatal(err)
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, mmapNodeExtentBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	for _, id := range ids {
		offset := int(mmapNodeTestPageOffset(id, 0))
		binary.LittleEndian.PutUint64(data[offset:offset+8], value)
	}
	if err := syscall.Munmap(data); err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	if err := file.Close(); err != nil {
		tb.Fatal(err)
	}
}

func openFlushBenchStoreWithTail(tb testing.TB, dir string, walSize int64, value []byte, iter int, tailRecords int) *Store {
	tb.Helper()

	store, err := Open(dir, Options{WALSize: walSize})
	if err != nil {
		tb.Fatal(err)
	}
	if err := store.Put(flushBenchKey('b', iter), value); err != nil {
		tb.Fatal(err)
	}
	if err := store.Close(); err != nil {
		tb.Fatal(err)
	}

	store, err = Open(dir, Options{WALSize: walSize})
	if err != nil {
		tb.Fatal(err)
	}
	base := iter * tailRecords
	for i := 0; i < tailRecords; i++ {
		if err := store.Put(flushBenchKey('t', base+i), value); err != nil {
			tb.Fatal(err)
		}
	}
	return store
}

func dirtyCloseFlushBenchStore(tb testing.TB, store *Store) {
	tb.Helper()

	stopCompactionDispatchersForTest(store)
	backend := store.backend
	store.backend = nil
	store.manifest = nil
	store.records = nil
	if err := backend.syncAndClose(); err != nil {
		tb.Fatal(err)
	}
}

func flushBenchWALSize(records int, valueLen int) int64 {
	return int64(walHeaderSize + records*(walRecordHeaderSize+flushBenchKeyLen+valueLen))
}

func flushBenchBytes(records int, valueLen int) int64 {
	return int64(records * (walRecordHeaderSize + flushBenchKeyLen + valueLen))
}

func flushBenchKey(tag byte, n int) []byte {
	key := make([]byte, flushBenchKeyLen)
	copy(key, "bench-key-")
	key[len("bench-key-")] = tag
	binary.BigEndian.PutUint64(key[flushBenchKeyLen-8:], uint64(n))
	return key
}
