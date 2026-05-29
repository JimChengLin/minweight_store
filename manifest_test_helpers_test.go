package minweight_store

import (
	"errors"
	"os"
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
	if err := validateManifestState(state); err != nil {
		return err
	}
	latest, ok, err := readManifestLog(path)
	if err != nil {
		return err
	}
	if !ok {
		return replaceManifest(path, state, 1)
	}
	seq, slot := nextManifestWrite(latest.seq, latest.slot)
	if slot == 0 {
		return replaceManifest(path, state, seq)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	var firstErr error
	firstErr = appendManifestRecord(file, state, seq, slot)
	if err := file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
