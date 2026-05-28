//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
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
			b.Run("direct_copy", func(b *testing.B) {
				src, dst := prepareMmapNodeStoreCopyBench(b, liveNodes)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := copyMmapNodeStoreDir(src, dst); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("if_different_equal", func(b *testing.B) {
				src, dst := prepareMmapNodeStoreCopyBench(b, liveNodes)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := copyMmapNodeStoreDirIfDifferent(src, dst); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
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
	tb.Helper()

	root := tb.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	nodes, err := openMmapNodeStore(src)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < liveNodes; i++ {
		_, page, err := nodes.Alloc()
		if err != nil {
			tb.Fatal(err)
		}
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
	return src, dst
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
