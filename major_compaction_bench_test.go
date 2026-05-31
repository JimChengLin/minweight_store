//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type majorCompactionBenchScenario struct {
	name          string
	ssts          int
	entriesPerSST int
	livePercent   int
	valueSize     int
}

var majorCompactionBenchScenarios = []majorCompactionBenchScenario{
	{name: "4sst/live80/entry256/value32", ssts: 4, entriesPerSST: 256, livePercent: 80, valueSize: 32},
	{name: "4sst/live50/entry1K/value32", ssts: 4, entriesPerSST: 1_000, livePercent: 50, valueSize: 32},
	{name: "4sst/live10/entry1K/value32", ssts: 4, entriesPerSST: 1_000, livePercent: 10, valueSize: 32},
	{name: "4sst/live80/entry256/value1K", ssts: 4, entriesPerSST: 256, livePercent: 80, valueSize: 1024},
	{name: "4sst/live10/entry1K/value1K", ssts: 4, entriesPerSST: 1_000, livePercent: 10, valueSize: 1024},
}

func BenchmarkStoreMajorCompaction(b *testing.B) {
	for _, scenario := range majorCompactionBenchScenarios {
		b.Run(scenario.name, func(b *testing.B) {
			data := newRecordBackendBenchDataWithValueSize(scenario.ssts*scenario.entriesPerSST, scenario.valueSize)
			benchmarkStoreMajorCompaction(b, data, scenario, Options{})
		})
	}
}

func BenchmarkStoreMajorCompactionWorkers(b *testing.B) {
	scenario := majorCompactionBenchScenario{
		name:          "8sst/live50/entry1K/value32",
		ssts:          8,
		entriesPerSST: 1_000,
		livePercent:   50,
		valueSize:     32,
	}
	benchmarkStoreMajorCompactionWorkers(b, scenario, 64<<10)
}

func BenchmarkStoreMajorCompactionWorkersValue1K(b *testing.B) {
	scenario := majorCompactionBenchScenario{
		name:          "8sst/live50/entry256/value1K",
		ssts:          8,
		entriesPerSST: 256,
		livePercent:   50,
		valueSize:     1024,
	}
	benchmarkStoreMajorCompactionWorkers(b, scenario, 256<<10)
}

func benchmarkStoreMajorCompactionWorkers(b *testing.B, scenario majorCompactionBenchScenario, targetSSTSize int64) {
	b.Helper()

	data := newRecordBackendBenchDataWithValueSize(scenario.ssts*scenario.entriesPerSST, scenario.valueSize)
	for _, workers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("workers%d", workers), func(b *testing.B) {
			benchmarkStoreMajorCompaction(b, data, scenario, Options{
				MajorCompactionThreadNum: workers,
				TargetSSTSize:            targetSSTSize,
			})
		})
	}
}

func benchmarkStoreMajorCompaction(b *testing.B, data recordBackendBenchData, scenario majorCompactionBenchScenario, options Options) {
	b.Helper()

	root := b.TempDir()
	b.SetBytes(majorCompactionBenchBytes(data, scenario.livePercent))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("major-%06d", i))
		store := prepareMajorCompactionBenchStore(b, dir, data, scenario, options)

		b.StartTimer()
		err := store.MajorCompact()
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
		closeForTest(b, store)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}

	totalEntries := float64(len(data.keys))
	liveEntries := totalEntries * float64(scenario.livePercent) / 100
	b.ReportMetric(totalEntries, "scanned_entries/op")
	b.ReportMetric(liveEntries, "live_entries/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*totalEntries), "ns/scanned_entry")
	if liveEntries != 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*liveEntries), "ns/live_entry")
	}
}

func prepareMajorCompactionBenchStore(b *testing.B, dir string, data recordBackendBenchData, scenario majorCompactionBenchScenario, options Options) *Store {
	b.Helper()

	options.WALSize = walBenchSize(data, 1)
	store, err := Open(dir, options)
	if err != nil {
		b.Fatal(err)
	}
	store.stopMinorCompactionDispatcher()

	for sst := 0; sst < scenario.ssts; sst++ {
		start := sst * scenario.entriesPerSST
		end := start + scenario.entriesPerSST
		for i := start; i < end; i++ {
			if err := store.Put(data.keys[i], data.values[i]); err != nil {
				b.Fatalf("put sst=%d entry=%d: %v", sst, i, err)
			}
		}
		if err := store.flush(); err != nil {
			b.Fatalf("flush sst=%d data wal: %v", sst, err)
		}
		sourceWALFileNo := store.checkpointWALFileNo
		compacted, err := store.minorCompactWAL(sourceWALFileNo)
		if err != nil || !compacted {
			b.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", sourceWALFileNo, compacted, err)
		}
		if err := store.flush(); err != nil {
			b.Fatalf("flush sst=%d install wal: %v", sst, err)
		}
	}

	staleStartInSST := scenario.entriesPerSST * scenario.livePercent / 100
	for sst := 0; sst < scenario.ssts; sst++ {
		start := sst * scenario.entriesPerSST
		for i := start + staleStartInSST; i < start+scenario.entriesPerSST; i++ {
			if err := store.Put(data.keys[i], updatedMinorCompactionBenchValue(data.values[i])); err != nil {
				b.Fatalf("put stale entry=%d: %v", i, err)
			}
		}
	}
	if staleStartInSST != scenario.entriesPerSST {
		if err := store.flush(); err != nil {
			b.Fatalf("flush stale wal: %v", err)
		}
	}
	return store
}

func majorCompactionBenchBytes(data recordBackendBenchData, livePercent int) int64 {
	liveEnd := len(data.keys) * livePercent / 100
	var total int64
	for i := 0; i < liveEnd; i++ {
		total += int64(len(data.keys[i]) + len(data.values[i]))
	}
	return total
}
