//go:build darwin || linux

package minweight_store

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/JimChengLin/minpatricia"
	"github.com/parquet-go/parquet-go"
)

type parquetRecordStoreBenchSize struct {
	name string
	n    int
}

type parquetRecordStoreBenchData struct {
	keys   [][]byte
	values [][]byte
}

type parquetRecordValue struct {
	Value []byte `parquet:"value"`
}

var parquetRecordStoreBenchSizes = []parquetRecordStoreBenchSize{
	{name: "1K", n: 1_000},
	{name: "10K", n: 10_000},
}

func BenchmarkParquetRecordStoreSequentialWrite(b *testing.B) {
	for _, size := range parquetRecordStoreBenchSizes {
		data := newParquetRecordStoreBenchData(size.n)
		b.Run(size.name, func(b *testing.B) {
			benchmarkParquetRecordStoreWrite(b, data, sequentialParquetRecordStoreBenchOrder(size.n))
		})
	}
}

func BenchmarkParquetRecordStoreRandomWrite(b *testing.B) {
	for _, size := range parquetRecordStoreBenchSizes {
		data := newParquetRecordStoreBenchData(size.n)
		b.Run(size.name, func(b *testing.B) {
			benchmarkParquetRecordStoreWrite(b, data, randomParquetRecordStoreBenchOrder(size.n))
		})
	}
}

func BenchmarkParquetRecordStoreRandomRead(b *testing.B) {
	for _, size := range parquetRecordStoreBenchSizes {
		data := newParquetRecordStoreBenchData(size.n)
		store, positions := buildParquetRecordStoreForTest(b, filepath.Join(b.TempDir(), "records.parquet"), recordsFromParquetBenchData(data))
		defer closeForTest(b, store)

		order := positionsByParquetRecordStoreBenchOrder(positions, randomParquetRecordStoreBenchOrder(size.n))
		benchmarkParquetRecordStoreReads(b, size.name, store, order)
	}
}

func BenchmarkParquetRecordStoreSequentialRead(b *testing.B) {
	for _, size := range parquetRecordStoreBenchSizes {
		data := newParquetRecordStoreBenchData(size.n)
		store, positions := buildParquetRecordStoreForTest(b, filepath.Join(b.TempDir(), "records.parquet"), recordsFromParquetBenchData(data))
		defer closeForTest(b, store)

		order := positionsByParquetRecordStoreBenchOrder(positions, sequentialParquetRecordStoreBenchOrder(size.n))
		benchmarkParquetRecordStoreReads(b, size.name, store, order)
	}
}

func BenchmarkParquetRecordStoreSeekSequentialRead(b *testing.B) {
	for _, size := range parquetRecordStoreBenchSizes {
		data := newParquetRecordStoreBenchData(size.n)
		store, _ := buildParquetRecordStoreForTest(b, filepath.Join(b.TempDir(), "records.parquet"), recordsFromParquetBenchData(data))
		defer closeForTest(b, store)

		b.Run(size.name+"/key", func(b *testing.B) {
			benchmarkParquetRecordStoreSeekSequentialRead[parquetRecordKey](b, store, func(record parquetRecordKey) int {
				return len(record.Key)
			})
		})
		b.Run(size.name+"/value", func(b *testing.B) {
			benchmarkParquetRecordStoreSeekSequentialRead[parquetRecordValue](b, store, func(record parquetRecordValue) int {
				return len(record.Value)
			})
		})
		b.Run(size.name+"/key_value", func(b *testing.B) {
			benchmarkParquetRecordStoreSeekSequentialRead[parquetRecord](b, store, func(record parquetRecord) int {
				return len(record.Key) + len(record.Value)
			})
		})
	}
}

func BenchmarkIndexBackendScanParquet(b *testing.B) {
	for _, size := range parquetRecordStoreBenchSizes {
		data := newParquetRecordStoreBenchData(size.n)
		store, positions := buildParquetRecordStoreForTest(b, filepath.Join(b.TempDir(), "records.parquet"), recordsFromParquetBenchData(data))
		defer closeForTest(b, store)

		records := parquetIndexRecordStore{parquetRecordStore: store}
		backend := newIndexBackendWithNodes(records, newHeapNodeStore())
		for i, key := range data.keys {
			if _, _, err := backend.index.Put(key, positions[i]); err != nil {
				b.Fatal(err)
			}
		}

		b.Run(size.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size.n * len(data.values[0])))
			b.ResetTimer()

			var sink int
			for i := 0; i < b.N; i++ {
				count := 0
				if err := backend.scan(func(item Item) bool {
					count++
					sink += len(item.Key) + len(item.Value)
					return true
				}); err != nil {
					b.Fatal(err)
				}
				if count != size.n {
					b.Fatalf("scan count = %d, want %d", count, size.n)
				}
			}
			_ = sink
		})
	}
}

func benchmarkParquetRecordStoreReads(b *testing.B, sizeName string, store *parquetRecordStore, positions []minpatricia.Position) {
	b.Helper()

	b.Run(sizeName+"/key", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		var sink int
		for i := 0; i < b.N; i++ {
			key, ok := store.Key(positions[i%len(positions)])
			if !ok {
				b.Fatal("missing key")
			}
			sink += len(key)
		}
		_ = sink
	})
	b.Run(sizeName+"/value", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		var sink int
		for i := 0; i < b.N; i++ {
			value, ok := store.Value(positions[i%len(positions)])
			if !ok {
				b.Fatal("missing value")
			}
			sink += len(value)
		}
		_ = sink
	})
	b.Run(sizeName+"/key_value", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		var sink int
		for i := 0; i < b.N; i++ {
			pos := positions[i%len(positions)]
			key, ok := store.Key(pos)
			if !ok {
				b.Fatal("missing key")
			}
			value, ok := store.Value(pos)
			if !ok {
				b.Fatal("missing value")
			}
			sink += len(key) + len(value)
		}
		_ = sink
	})
}

type parquetIndexRecordStore struct {
	*parquetRecordStore
}

func (s parquetIndexRecordStore) Append(key, value []byte) (minpatricia.Position, error) {
	return 0, ErrClosed
}

func (s parquetIndexRecordStore) Delete(key []byte) (minpatricia.Position, error) {
	return 0, ErrClosed
}

func (s parquetIndexRecordStore) Free(pos minpatricia.Position) error {
	return nil
}

func (s parquetIndexRecordStore) Sync() error {
	return nil
}

func benchmarkParquetRecordStoreSeekSequentialRead[T any](b *testing.B, store *parquetRecordStore, recordSize func(T) int) {
	b.Helper()

	recordsPerScan := store.Len()
	buffer := make([]T, 256)
	b.ReportAllocs()
	b.ResetTimer()

	var sink int
	for i := 0; i < b.N; i++ {
		for _, group := range store.rowGroups {
			reader := parquet.NewGenericRowGroupReader[T](group)
			if err := reader.SeekToRow(0); err != nil {
				b.Fatal(err)
			}
			for {
				n, err := reader.Read(buffer)
				for _, record := range buffer[:n] {
					sink += recordSize(record)
				}
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()

	_ = sink
	totalRecords := float64(b.N) * float64(recordsPerScan)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/totalRecords, "ns/record")
}

func benchmarkParquetRecordStoreWrite(b *testing.B, data parquetRecordStoreBenchData, order []int) {
	b.Helper()

	path := filepath.Join(b.TempDir(), "records.parquet")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store, err := createParquetRecordStore(path, parquetRecordTestFileNo)
		if err != nil {
			b.Fatal(err)
		}
		for _, j := range order {
			if _, err := store.Append(data.keys[j], data.values[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := store.Sync(); err != nil {
			b.Fatal(err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	}

	totalRecords := float64(b.N) * float64(len(order))
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/totalRecords, "ns/record")
}

func newParquetRecordStoreBenchData(n int) parquetRecordStoreBenchData {
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

	return parquetRecordStoreBenchData{
		keys:   keys,
		values: values,
	}
}

func recordsFromParquetBenchData(data parquetRecordStoreBenchData) []parquetRecord {
	records := make([]parquetRecord, len(data.keys))
	for i := range records {
		records[i] = parquetRecord{
			Key:   data.keys[i],
			Value: data.values[i],
		}
	}
	return records
}

func sequentialParquetRecordStoreBenchOrder(n int) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	return order
}

func randomParquetRecordStoreBenchOrder(n int) []int {
	order := sequentialParquetRecordStoreBenchOrder(n)
	rng := rand.New(rand.NewSource(int64(n) << 1))
	rng.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
	return order
}

func positionsByParquetRecordStoreBenchOrder(positions []minpatricia.Position, order []int) []minpatricia.Position {
	ordered := make([]minpatricia.Position, len(order))
	for i, j := range order {
		ordered[i] = positions[j]
	}
	return ordered
}
