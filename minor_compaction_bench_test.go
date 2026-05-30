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

type minorCompactionBenchScenario struct {
	name        string
	entries     int
	livePercent int
	valueSize   int
}

var minorCompactionBenchScenarios = []minorCompactionBenchScenario{
	{name: "1K/live100/value32", entries: 1_000, livePercent: 100, valueSize: 32},
	{name: "10K/live50/value32", entries: 10_000, livePercent: 50, valueSize: 32},
	{name: "10K/live10/value32", entries: 10_000, livePercent: 10, valueSize: 32},
	{name: "1K/live100/value1K", entries: 1_000, livePercent: 100, valueSize: 1024},
	{name: "1K/live0/value32", entries: 1_000, livePercent: 0, valueSize: 32},
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

func BenchmarkStoreMinorCompactionLiveRatio(b *testing.B) {
	for _, scenario := range minorCompactionBenchScenarios {
		b.Run(scenario.name, func(b *testing.B) {
			data := newRecordBackendBenchDataWithValueSize(scenario.entries, scenario.valueSize)
			benchmarkStoreMinorCompactionSourceWAL(b, data, firstWALSegmentNo, func(b *testing.B, dir string) *Store {
				return prepareMinorCompactionRatioWALBenchStore(b, dir, data, scenario.livePercent)
			})
		})
	}
}

func BenchmarkStoreMinorCompactionWorkers(b *testing.B) {
	for _, workers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("workers%d", workers), func(b *testing.B) {
			data := newRecordBackendBenchDataWithValueSize(1_000, 32)
			benchmarkStoreMinorCompaction(b, data, workers, prepareMinorCompactionMultiWALBenchStore)
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

func benchmarkStoreMinorCompaction(b *testing.B, data recordBackendBenchData, workers int, prepare func(*testing.B, string, recordBackendBenchData, int) *Store) {
	b.Helper()

	root := b.TempDir()
	bytesPerOp := minorCompactionBenchBytes(data) * 3
	b.SetBytes(bytesPerOp)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("minor-workers-%06d", i))
		store := prepare(b, dir, data, workers)

		b.StartTimer()
		err := store.minorCompact()
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
		closeForTest(b, store)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}

	entries := float64(len(data.keys) * 3)
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

func prepareMinorCompactionRatioWALBenchStore(b *testing.B, dir string, data recordBackendBenchData, livePercent int) *Store {
	b.Helper()

	store := prepareMinorCompactionLiveWALBenchStore(b, dir, data)
	staleStart := len(data.keys) * livePercent / 100
	for i := staleStart; i < len(data.keys); i++ {
		if err := store.Put(data.keys[i], updatedMinorCompactionBenchValue(data.values[i])); err != nil {
			b.Fatal(err)
		}
	}
	if staleStart != len(data.keys) {
		if err := store.flush(); err != nil {
			b.Fatal(err)
		}
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

func prepareMinorCompactionMultiWALBenchStore(b *testing.B, dir string, data recordBackendBenchData, workers int) *Store {
	b.Helper()

	store, err := Open(dir, Options{
		WALSize:                  walBenchSize(data, 1) + int64(len(data.keys)*4),
		MinorCompactionThreadNum: workers,
		MaxImmutableWALNum:       1,
	})
	if err != nil {
		b.Fatal(err)
	}
	store.stopMinorCompactionDispatcher()
	for batch := 0; batch < 4; batch++ {
		for i, key := range data.keys {
			value := data.values[i]
			if batch != 0 {
				value = updatedMinorCompactionBenchValue(value)
			}
			if err := store.Put(appendMinorCompactionBatchToKey(key, batch), value); err != nil {
				b.Fatal(err)
			}
		}
		if err := store.flush(); err != nil {
			b.Fatal(err)
		}
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

func newRecordBackendBenchDataWithValueSize(n, valueSize int) recordBackendBenchData {
	data := newRecordBackendBenchData(n)
	for i, key := range data.keys {
		value := make([]byte, valueSize)
		prefix := make([]byte, 0, len("value-")+len(key))
		prefix = append(prefix, "value-"...)
		prefix = append(prefix, key...)
		copy(value, prefix)
		for j := len(prefix); j < len(value); j++ {
			value[j] = byte('a' + j%26)
		}
		data.values[i] = value
	}
	return data
}

func updatedMinorCompactionBenchValue(value []byte) []byte {
	updated := make([]byte, len(value))
	copy(updated, value)
	if len(updated) != 0 {
		updated[0] ^= 0xff
	}
	return updated
}

func appendMinorCompactionBatchToKey(key []byte, batch int) []byte {
	out := make([]byte, 0, len(key)+4)
	out = append(out, key...)
	out = fmt.Appendf(out, "-%02d", batch)
	return out
}
