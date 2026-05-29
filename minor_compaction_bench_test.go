//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type minorCompactionBenchSize struct {
	name string
	n    int
}

var minorCompactionBenchSizes = []minorCompactionBenchSize{
	{name: "64", n: 64},
	{name: "1K", n: 1_000},
	{name: "10K", n: 10_000},
}

func BenchmarkStoreMinorCompactionLiveWALEntries(b *testing.B) {
	for _, size := range minorCompactionBenchSizes {
		data := newRecordBackendBenchData(size.n)
		b.Run(size.name, func(b *testing.B) {
			benchmarkStoreMinorCompactionSourceWAL(b, data, firstWALSegmentNo, func(b *testing.B, dir string) *Store {
				return prepareMinorCompactionLiveWALBenchStore(b, dir, data)
			})
		})
	}
}

func BenchmarkStoreMinorCompactionDeleteOnlyWALEntries(b *testing.B) {
	for _, size := range minorCompactionBenchSizes {
		data := newRecordBackendBenchData(size.n)
		b.Run(size.name, func(b *testing.B) {
			benchmarkStoreMinorCompactionSourceWAL(b, data, firstWALSegmentNo+1, func(b *testing.B, dir string) *Store {
				return prepareMinorCompactionDeleteOnlyWALBenchStore(b, dir, data)
			})
		})
	}
}

func benchmarkStoreMinorCompactionSourceWAL(b *testing.B, data recordBackendBenchData, sourceWALFileNo uint64, prepare func(*testing.B, string) *Store) {
	b.Helper()

	root := b.TempDir()
	bytesPerOp := minorCompactionBenchBytes(data)
	b.SetBytes(bytesPerOp)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("minor-%06d", i))
		store := prepare(b, dir)

		b.StartTimer()
		compacted, err := store.minorCompactWAL(sourceWALFileNo)
		b.StopTimer()

		if err != nil || !compacted {
			b.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", sourceWALFileNo, compacted, err)
		}
		closeForTest(b, store)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}

	entries := float64(len(data.keys))
	b.ReportMetric(entries, "entries/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*entries), "ns/entry")
}

func prepareMinorCompactionLiveWALBenchStore(b *testing.B, dir string, data recordBackendBenchData) *Store {
	b.Helper()

	store, err := Open(dir, Options{
		WALSize:            walBenchSize(data, 1),
		MaxImmutableWALNum: 0,
	})
	if err != nil {
		b.Fatal(err)
	}
	loadStoreBenchData(b, store, data)
	if err := store.flush(); err != nil {
		b.Fatal(err)
	}
	return store
}

func prepareMinorCompactionDeleteOnlyWALBenchStore(b *testing.B, dir string, data recordBackendBenchData) *Store {
	b.Helper()

	store := prepareMinorCompactionLiveWALBenchStore(b, dir, data)
	for _, key := range data.keys {
		deleted, err := store.Delete(key)
		if err != nil || !deleted {
			b.Fatalf("Delete(%q) = (%v,%v), want true,nil", key, deleted, err)
		}
	}
	if err := store.flush(); err != nil {
		b.Fatal(err)
	}
	return store
}

func minorCompactionBenchBytes(data recordBackendBenchData) int64 {
	var total int64
	for i, key := range data.keys {
		total += int64(len(key) + len(data.values[i]))
	}
	return total
}
