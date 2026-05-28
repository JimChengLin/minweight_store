//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

type recordBackendBenchData struct {
	keys   [][]byte
	values [][]byte
}

type recordBackendBenchFixture struct {
	heap      *heapRecordStore
	positions []minpatricia.Position
}

func BenchmarkRecordStoreRead(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newRecordBackendBenchData(size.n)
		b.Run(size.name+"/heap", func(b *testing.B) {
			fixture := buildHeapRecordStoreBenchFixture(data)
			b.ReportAllocs()
			b.ResetTimer()

			var sink int
			for i := 0; i < b.N; i++ {
				pos := fixture.positions[i%len(fixture.positions)]
				key, ok := fixture.keysValue(pos)
				if !ok {
					b.Fatal("missing key")
				}
				value, ok := fixture.value(pos)
				if !ok {
					b.Fatal("missing value")
				}
				sink += len(key) + len(value)
			}
			_ = sink
		})
		b.Run(size.name+"/wal_mmap", func(b *testing.B) {
			wal, fixture := buildMmapWALRecordStoreBenchFixture(b, data)
			defer closeForTest(b, wal)
			b.ReportAllocs()
			b.ResetTimer()

			var sink int
			for i := 0; i < b.N; i++ {
				pos := fixture.positions[i%len(fixture.positions)]
				key, ok := wal.Key(pos)
				if !ok {
					b.Fatal("missing key")
				}
				value, ok := wal.Value(pos)
				if !ok {
					b.Fatal("missing value")
				}
				sink += len(key) + len(value)
			}
			_ = sink
		})
	}
}

func BenchmarkRecordStorePutBatch(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newRecordBackendBenchData(size.n)
		b.Run(size.name+"/heap", func(b *testing.B) {
			benchmarkRecordStoreBatch(b, size.n, func() {
				records := newHeapRecordStore()
				for i, key := range data.keys {
					if _, err := records.Append(key, data.values[i]); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
		b.Run(size.name+"/wal_mmap", func(b *testing.B) {
			wal, err := openMmapWALRecordStore(filepath.Join(b.TempDir(), "wal"), walBenchSize(data, 1))
			if err != nil {
				b.Fatal(err)
			}
			defer closeForTest(b, wal)

			benchmarkRecordStoreBatch(b, size.n, func() {
				wal.used = walHeaderSize
				wal.writeUsed()
				for i, key := range data.keys {
					if _, err := wal.Append(key, data.values[i]); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkStoreGetRecordBackend(b *testing.B) {
	for _, size := range nodeStoreBenchSizes {
		data := newRecordBackendBenchData(size.n)
		b.Run(size.name+"/heap", func(b *testing.B) {
			store := New()
			loadStoreBenchData(b, store, data)
			b.ReportAllocs()
			b.ResetTimer()

			var sink int
			for i := 0; i < b.N; i++ {
				value, ok, err := store.Get(data.keys[i%len(data.keys)])
				if err != nil || !ok {
					b.Fatalf("Get failed: ok=%v err=%v", ok, err)
				}
				sink += len(value)
			}
			_ = sink
		})
		b.Run(size.name+"/wal_mmap", func(b *testing.B) {
			store, err := Open(b.TempDir(), Options{WALSize: walBenchSize(data, 1)})
			if err != nil {
				b.Fatal(err)
			}
			defer closeForTest(b, store)
			loadStoreBenchData(b, store, data)
			b.ReportAllocs()
			b.ResetTimer()

			var sink int
			for i := 0; i < b.N; i++ {
				value, ok, err := store.Get(data.keys[i%len(data.keys)])
				if err != nil || !ok {
					b.Fatalf("Get failed: ok=%v err=%v", ok, err)
				}
				sink += len(value)
			}
			_ = sink
		})
	}
}

func newRecordBackendBenchData(n int) recordBackendBenchData {
	rng := rand.New(rand.NewSource(int64(n)))
	keys := make([][]byte, 0, n)
	values := make([][]byte, 0, n)
	seen := make(map[string]struct{}, n)

	for len(keys) < n {
		key := fmt.Sprintf("key-%08x-%08x", rng.Uint32(), rng.Uint32())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		keys = append(keys, []byte(key))
		values = append(values, []byte("value-"+key))
	}

	return recordBackendBenchData{
		keys:   keys,
		values: values,
	}
}

func buildHeapRecordStoreBenchFixture(data recordBackendBenchData) recordBackendBenchFixture {
	records := newHeapRecordStore()
	positions := make([]minpatricia.Position, len(data.keys))
	for i, key := range data.keys {
		pos, err := records.Append(key, data.values[i])
		if err != nil {
			panic(err)
		}
		positions[i] = pos
	}
	return recordBackendBenchFixture{
		heap:      records,
		positions: positions,
	}
}

func buildMmapWALRecordStoreBenchFixture(tb testing.TB, data recordBackendBenchData) (*mmapWALRecordStore, recordBackendBenchFixture) {
	tb.Helper()

	wal, err := openMmapWALRecordStore(filepath.Join(tb.TempDir(), "wal"), walBenchSize(data, 1))
	if err != nil {
		tb.Fatal(err)
	}
	positions := make([]minpatricia.Position, len(data.keys))
	for i, key := range data.keys {
		pos, err := wal.Append(key, data.values[i])
		if err != nil {
			tb.Fatal(err)
		}
		positions[i] = pos
	}
	return wal, recordBackendBenchFixture{
		positions: positions,
	}
}

func benchmarkRecordStoreBatch(b *testing.B, items int, runBatch func()) {
	b.Helper()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBatch()
	}
	b.StopTimer()

	totalItems := float64(b.N) * float64(items)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/totalItems, "ns/op")
}

func loadStoreBenchData(tb testing.TB, store *Store, data recordBackendBenchData) {
	tb.Helper()

	for i, key := range data.keys {
		if err := store.Put(key, data.values[i]); err != nil {
			tb.Fatal(err)
		}
	}
}

func walBenchSize(data recordBackendBenchData, batches int) int64 {
	var bytes int64 = walHeaderSize
	for i, key := range data.keys {
		bytes += int64(walRecordHeaderSize + len(key) + len(data.values[i]))
	}
	if batches < 1 {
		batches = 1
	}
	return bytes*int64(batches) + 4096
}

func (f recordBackendBenchFixture) keysValue(pos minpatricia.Position) ([]byte, bool) {
	return f.heap.Key(pos)
}

func (f recordBackendBenchFixture) value(pos minpatricia.Position) ([]byte, bool) {
	return f.heap.Value(pos)
}
