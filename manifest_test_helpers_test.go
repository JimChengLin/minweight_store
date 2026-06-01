package minweight_store

import (
	"errors"
	"os"
	"testing"
)

func readManifest(path string) (manifestState, bool, error) {
	record, ok, err := readManifestLog(path)
	if err != nil || !ok {
		return manifestState{}, ok, err
	}
	return record.state, true, nil
}

func readManifestLog(path string) (manifestRecord, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return manifestRecord{}, false, nil
	}
	if err != nil {
		return manifestRecord{}, false, err
	}
	defer file.Close()
	record, err := readManifestLogFile(file)
	if err != nil {
		return manifestRecord{}, false, err
	}
	return record, true, nil
}

func writeManifest(path string, state manifestState) error {
	latest, ok, err := readManifestLog(path)
	if err != nil {
		return err
	}
	if !ok {
		return replaceManifest(path, state, 1)
	}
	seq := latest.seq + 1
	offset := latest.offset + latest.size
	record, err := encodeManifestRecord(state, seq)
	if err != nil {
		return err
	}
	if offset+len(record) > manifestSize {
		return replaceManifest(path, state, seq)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	var firstErr error
	firstErr = appendManifestRecord(file, record, offset)
	if err := file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func replaceManifest(path string, state manifestState, seq uint64) error {
	record, err := encodeManifestRecord(state, seq)
	if err != nil {
		return err
	}
	return replaceManifestRecord(path, record)
}

func manifestLiveSSTDeletedEntriesForTest(t *testing.T, path string) map[uint64]uint64 {
	t.Helper()

	state, ok, err := readManifest(path)
	if err != nil || !ok {
		t.Fatalf("readManifest(%s) = (%+v,%v,%v), want state,true,nil", path, state, ok, err)
	}
	deletedEntries := make(map[uint64]uint64, len(state.liveSSTs))
	for _, sst := range state.liveSSTs {
		deletedEntries[sst.fileNo] = sst.deletedEntries
	}
	return deletedEntries
}

func assertManifestLiveSSTDeletedEntriesForTest(t *testing.T, path string, fileNo, want uint64) {
	t.Helper()

	got, ok := manifestLiveSSTDeletedEntriesForTest(t, path)[fileNo]
	if !ok {
		t.Fatalf("manifest live SST %d missing", fileNo)
	}
	if got != want {
		t.Fatalf("manifest live SST %d deleted entries = %d, want %d", fileNo, got, want)
	}
}
