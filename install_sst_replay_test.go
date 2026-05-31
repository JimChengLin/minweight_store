//go:build darwin || linux

package minweight_store

import (
	"testing"

	"github.com/JimChengLin/minpatricia"
)

type countingKeyRecordStore struct {
	keys     map[minpatricia.Position][]byte
	keyCalls int
}

func (s *countingKeyRecordStore) Key(pos minpatricia.Position) ([]byte, bool) {
	s.keyCalls++
	key, ok := s.keys[pos]
	return key, ok
}

type countingIndexEntry struct {
	indexKey []byte
	indexPos minpatricia.Position
}

func newCountingIndexForTest(t *testing.T, keys map[minpatricia.Position][]byte, entries []countingIndexEntry) (*minpatricia.Index, *countingKeyRecordStore) {
	t.Helper()

	records := &countingKeyRecordStore{keys: keys}
	index := minpatricia.NewWithNodes(records, newHeapNodeStore())
	for _, entry := range entries {
		if _, _, err := index.Put(entry.indexKey, entry.indexPos); err != nil {
			t.Fatal(err)
		}
	}
	records.keyCalls = 0
	return index, records
}

func TestApplyInstallSSTRecordWithLiveIndexDoesNotReadRecordKeys(t *testing.T) {
	dir := t.TempDir()
	store := openMinorCompactionStoreInDirForTest(t, dir)
	defer closeForTest(t, store)

	sstFileNo, candidates := buildParquetFromWALForTest(t, store, firstWALSegmentNo)
	if len(candidates) == 0 {
		t.Fatal("install SST test needs live WAL candidates")
	}
	keys := make(map[minpatricia.Position][]byte, len(candidates)*2)
	secondaryEntries := make([]countingIndexEntry, 0, len(candidates))
	liveEntries := make([]countingIndexEntry, 0, len(candidates))
	for rowIndex, candidate := range candidates {
		newPos, err := makeParquetRecordPosition(sstFileNo, uint64(rowIndex))
		if err != nil {
			t.Fatal(err)
		}
		keys[candidate.pos] = candidate.key
		keys[newPos] = candidate.key
		secondaryEntries = append(secondaryEntries, countingIndexEntry{indexKey: candidate.key, indexPos: candidate.pos})
		liveEntries = append(liveEntries, countingIndexEntry{indexKey: candidate.key, indexPos: newPos})
	}
	secondary, secondaryRecords := newCountingIndexForTest(t, keys, secondaryEntries)
	live, liveRecords := newCountingIndexForTest(t, keys, liveEntries)

	if err := applyInstallSSTRecord(store.records, secondary, live, firstWALSegmentNo, sstFileNo); err != nil {
		t.Fatal(err)
	}
	for _, entry := range liveEntries {
		pos, ok, err := secondary.Probe(entry.indexKey)
		if err != nil || !ok || pos != entry.indexPos {
			t.Fatalf("secondary.Probe(%q) = (%d,%v,%v), want %d,true,nil", entry.indexKey, pos, ok, err, entry.indexPos)
		}
	}
	if secondaryRecords.keyCalls != 0 || liveRecords.keyCalls != 0 {
		t.Fatalf("RecordStore.Key calls = secondary %d live %d, want 0,0", secondaryRecords.keyCalls, liveRecords.keyCalls)
	}
}

func TestApplyInstallSSTBatchRecordWithLiveIndexDoesNotReadRecordKeys(t *testing.T) {
	dir := t.TempDir()
	store := openMajorCompactionStoreForTest(t, dir)
	defer closeForTest(t, store)

	oldSSTFileNos := parquetFileNosForTest(store)
	newSSTs, entries, err := store.buildMajorCompactionSSTs(oldSSTFileNos)
	if err != nil {
		t.Fatal(err)
	}
	newSSTsOwnershipTransferred := false
	defer func() {
		if !newSSTsOwnershipTransferred {
			_ = cleanupMajorCompactionSSTs(newSSTs)
		}
	}()
	if err := store.records.installParquetSegments(newSSTs); err != nil {
		t.Fatal(err)
	}
	newSSTsOwnershipTransferred = true
	if len(entries) == 0 {
		t.Fatal("install SST batch test needs live SST entries")
	}

	keys := make(map[minpatricia.Position][]byte, len(entries)*2)
	secondaryEntries := make([]countingIndexEntry, 0, len(entries))
	liveEntries := make([]countingIndexEntry, 0, len(entries))
	for _, entry := range entries {
		keys[entry.oldPos] = entry.key
		keys[entry.newPos] = entry.key
		secondaryEntries = append(secondaryEntries, countingIndexEntry{indexKey: entry.key, indexPos: entry.oldPos})
		liveEntries = append(liveEntries, countingIndexEntry{indexKey: entry.key, indexPos: entry.newPos})
	}
	secondary, secondaryRecords := newCountingIndexForTest(t, keys, secondaryEntries)
	live, liveRecords := newCountingIndexForTest(t, keys, liveEntries)

	if err := applyInstallSSTBatchRecord(store.records, secondary, live, oldSSTFileNos, parquetStoreFileNos(newSSTs)); err != nil {
		t.Fatal(err)
	}
	for _, entry := range liveEntries {
		pos, ok, err := secondary.Probe(entry.indexKey)
		if err != nil || !ok || pos != entry.indexPos {
			t.Fatalf("secondary.Probe(%q) = (%d,%v,%v), want %d,true,nil", entry.indexKey, pos, ok, err, entry.indexPos)
		}
	}
	if secondaryRecords.keyCalls != 0 || liveRecords.keyCalls != 0 {
		t.Fatalf("RecordStore.Key calls = secondary %d live %d, want 0,0", secondaryRecords.keyCalls, liveRecords.keyCalls)
	}
}
