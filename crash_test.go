//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/JimChengLin/minpatricia"
)

const crashTestWALSize int64 = 8 << 10

func TestStoreCrashRecoveryChaos(t *testing.T) {
	t.Run("compaction_path", func(t *testing.T) {
		runCrashRecoveryProgram(t, []byte{
			0, 22, 4,
			33, 55, 4,
			9, 4,
			10, 5,
		}, false)
	})
	t.Run("install_skip_windows", func(t *testing.T) {
		runCrashRecoveryProgram(t, []byte{
			0, 17, 4,
			34, 51, 4,
			22, 250,
			55, 4,
			9, 4,
			251,
		}, true)
	})
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed_%02d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			program := make([]byte, 32)
			if _, err := rng.Read(program); err != nil {
				t.Fatal(err)
			}
			runCrashRecoveryProgram(t, program, false)
		})
	}
}

func FuzzStoreCrashRecovery(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{9, 17, 25, 33, 41, 49, 57, 65})
	f.Add([]byte{255, 254, 128, 127, 64, 63, 32, 31})
	f.Add([]byte{0, 17, 4, 34, 51, 4, 250, 55, 4, 9, 4, 251})

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 48 {
			program = program[:48]
		}
		runCrashRecoveryProgram(t, program, true)
	})
}

func runCrashRecoveryProgram(t *testing.T, program []byte, allowInjectedWindows bool) {
	t.Helper()

	dir := t.TempDir()
	store := openCrashProgramStore(t, dir)
	defer func() {
		if store != nil && store.backend != nil {
			closeForTest(t, store)
		}
	}()

	expected := make(map[string]string)
	for step, op := range program {
		key := fmt.Sprintf("key-%02d", int(op>>4)%8)
		if allowInjectedWindows {
			switch op {
			case 250:
				store = crashInstallSSTSkipWindow(t, store, dir, expected, step)
				assertLiveSSTStatsForStore(t, store)
				continue
			case 251:
				store = crashInstallSSTBatchSkipWindow(t, store, dir, expected, step)
				assertLiveSSTStatsForStore(t, store)
				continue
			}
		}
		switch op % 11 {
		case 0, 1, 2:
			value := fmt.Sprintf("value-%03d-%02x", step, op)
			if err := store.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("step %d put %s: %v", step, key, err)
			}
			expected[key] = value
		case 3:
			if _, err := store.Delete([]byte(key)); err != nil {
				t.Fatalf("step %d delete %s: %v", step, key, err)
			}
			delete(expected, key)
		case 4:
			if err := store.flush(); err != nil {
				t.Fatalf("step %d flush: %v", step, err)
			}
		case 5:
			dirtySyncAndCloseStoreForTest(t, store)
			store = reopenCrashTestStore(t, dir, expected)
		case 6:
			if activeWALHasRecords(store) {
				simulatePrimaryWALFlushedCheckpointForTest(t, store)
				store = reopenCrashTestStore(t, dir, expected)
			}
		case 7:
			if activeWALHasRecords(store) {
				simulateCheckpointAfterSecondaryReplayBeforeManifestForTest(t, store)
				store = reopenCrashTestStore(t, dir, expected)
			}
		case 8:
			if err := store.Close(); err != nil {
				t.Fatalf("step %d close: %v", step, err)
			}
			store = reopenCrashTestStore(t, dir, expected)
		case 9:
			if err := store.minorCompact(); err != nil {
				t.Fatalf("step %d minor compact: %v", step, err)
			}
		case 10:
			if err := store.MajorCompact(); err != nil {
				t.Fatalf("step %d major compact: %v", step, err)
			}
		}
		assertLiveSSTStatsForStore(t, store)
	}
	assertCrashStoreContents(t, store, expected)
	assertLiveSSTStatsForStore(t, store)
}

func crashInstallSSTSkipWindow(t *testing.T, store *Store, dir string, expected map[string]string, step int) *Store {
	t.Helper()

	sourceWALFileNo, ok := firstCompactableWALForCrashTest(store)
	if !ok {
		return store
	}
	sstFileNo, entries := buildParquetFromWALForTest(t, store, sourceWALFileNo)
	if len(entries) == 0 {
		if err := store.records.scheduleSSTDelete(sstFileNo); err != nil {
			t.Fatalf("step %d schedule empty install-sst parquet delete: %v", step, err)
		}
		if err := store.records.deletePendingSSTs(); err != nil {
			t.Fatalf("step %d delete empty install-sst parquet: %v", step, err)
		}
		return store
	}
	key := string(entries[0].key)
	value := fmt.Sprintf("chaos-install-skip-%03d", step)
	if err := store.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("step %d install-sst skip put %s: %v", step, key, err)
	}
	expected[key] = value
	if _, err := store.records.AppendInstallSSTRecord(sourceWALFileNo, sstFileNo); err != nil {
		t.Fatalf("step %d append install_sst: %v", step, err)
	}
	dirtySyncAndCloseStoreForTest(t, store)
	return reopenCrashTestStore(t, dir, expected)
}

func crashInstallSSTBatchSkipWindow(t *testing.T, store *Store, dir string, expected map[string]string, step int) *Store {
	t.Helper()

	oldSSTFileNos, err := store.majorCompactionSSTFileNos()
	if err != nil {
		t.Fatalf("step %d major candidate: %v", step, err)
	}
	if len(oldSSTFileNos) == 0 {
		return store
	}
	newSSTs, entries, err := store.buildMajorCompactionSSTs(oldSSTFileNos)
	if err != nil {
		t.Fatalf("step %d build major SSTs: %v", step, err)
	}
	if len(entries) == 0 {
		_ = cleanupMajorCompactionSSTs(newSSTs)
		return store
	}
	if err := store.records.installParquetSegments(newSSTs); err != nil {
		_ = cleanupMajorCompactionSSTs(newSSTs)
		t.Fatalf("step %d install built major SSTs: %v", step, err)
	}
	key := string(entries[0].key)
	value := fmt.Sprintf("chaos-batch-skip-%03d", step)
	if err := store.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("step %d install-sst-batch skip put %s: %v", step, key, err)
	}
	expected[key] = value
	if _, err := store.records.AppendInstallSSTBatchRecord(oldSSTFileNos, parquetStoreFileNos(newSSTs)); err != nil {
		t.Fatalf("step %d append install_sst_batch: %v", step, err)
	}
	dirtySyncAndCloseStoreForTest(t, store)
	return reopenCrashTestStore(t, dir, expected)
}

func firstCompactableWALForCrashTest(store *Store) (uint64, bool) {
	store.primaryMu.RLock()
	defer store.primaryMu.RUnlock()

	if store.records == nil {
		return 0, false
	}
	fileNos := store.records.compactableWALFileNos(store.checkpointWALFileNo, 0)
	if len(fileNos) == 0 {
		return 0, false
	}
	return fileNos[0], true
}

func openCrashProgramStore(t *testing.T, dir string) *Store {
	t.Helper()

	store, err := Open(dir, Options{
		WALSize:                  crashTestWALSize,
		MajorCompactionThreadNum: 2,
		TargetSSTSize:            1,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopCompactionDispatchersForTest(store)
	return store
}

func reopenCrashTestStore(t *testing.T, dir string, expected map[string]string) *Store {
	t.Helper()

	store := openCrashProgramStore(t, dir)
	assertCrashStoreContents(t, store, expected)
	assertLiveSSTStatsForStore(t, store)
	return store
}

func activeWALHasRecords(store *Store) bool {
	if store == nil {
		return false
	}
	store.primaryMu.RLock()
	defer store.primaryMu.RUnlock()
	if store.records == nil {
		return false
	}
	active := store.records.activeSegment()
	return active != nil && active.used != walHeaderSize
}

func assertCrashStoreContents(t *testing.T, store *Store, expected map[string]string) {
	t.Helper()

	for key, want := range expected {
		assertGet(t, store, key, want)
	}

	seen := make(map[string]struct{}, len(expected))
	var mismatch string
	if err := store.Scan(func(item Item) bool {
		key := string(item.Key)
		value := string(item.Value)
		want, ok := expected[key]
		if !ok {
			mismatch = fmt.Sprintf("unexpected item %s=%s", key, value)
			return false
		}
		if value != want {
			mismatch = fmt.Sprintf("item %s=%s, want %s", key, value, want)
			return false
		}
		seen[key] = struct{}{}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if mismatch != "" {
		t.Fatal(mismatch)
	}
	if len(seen) != len(expected) {
		t.Fatalf("Scan saw %d items, want %d", len(seen), len(expected))
	}
}

func assertLiveSSTStatsForStore(t *testing.T, store *Store) {
	t.Helper()

	if store == nil {
		return
	}
	store.primaryMu.RLock()
	backend := store.backend
	records := store.records
	if backend == nil || records == nil {
		store.primaryMu.RUnlock()
		return
	}
	liveByFileNo := make(map[uint64]uint64)
	err := backend.index.Ascend(func(_ []byte, pos minpatricia.Position) bool {
		liveByFileNo[recordPositionFileNo(pos)]++
		return true
	})
	store.primaryMu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	records.mu.RLock()
	defer records.mu.RUnlock()
	for fileNo, stats := range records.liveSSTs {
		sst, ok := records.segments[fileNo].(*parquetRecordStore)
		if !ok {
			t.Fatalf("live SST %d segment is %T, want parquetRecordStore", fileNo, records.segments[fileNo])
		}
		totalEntries := uint64(sst.Len())
		if stats.totalEntries != totalEntries {
			t.Fatalf("live SST %d total_entries = %d, want parquet Len %d", fileNo, stats.totalEntries, totalEntries)
		}
		wantDeleted := totalEntries - liveByFileNo[fileNo]
		if stats.deletedEntries != wantDeleted {
			t.Fatalf("live SST %d deleted_entries = %d, want %d", fileNo, stats.deletedEntries, wantDeleted)
		}
	}
}
