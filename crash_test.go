//go:build darwin || linux

package minweight_store

import (
	"fmt"
	"math/rand"
	"testing"
)

const crashTestWALSize int64 = 8 << 10

func TestStoreCrashRecoveryChaos(t *testing.T) {
	t.Run("compaction_path", func(t *testing.T) {
		runCrashRecoveryProgram(t, []byte{
			0, 22, 4,
			33, 55, 4,
			9, 4,
			10, 5,
		})
	})
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed_%02d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			program := make([]byte, 32)
			if _, err := rng.Read(program); err != nil {
				t.Fatal(err)
			}
			runCrashRecoveryProgram(t, program)
		})
	}
}

func FuzzStoreCrashRecovery(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{9, 17, 25, 33, 41, 49, 57, 65})
	f.Add([]byte{255, 254, 128, 127, 64, 63, 32, 31})

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 48 {
			program = program[:48]
		}
		runCrashRecoveryProgram(t, program)
	})
}

func runCrashRecoveryProgram(t *testing.T, program []byte) {
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
	}
	assertCrashStoreContents(t, store, expected)
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
	store.stopMinorCompactionDispatcher()
	return store
}

func reopenCrashTestStore(t *testing.T, dir string, expected map[string]string) *Store {
	t.Helper()

	store := openCrashProgramStore(t, dir)
	assertCrashStoreContents(t, store, expected)
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
