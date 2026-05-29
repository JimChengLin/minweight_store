package minweight_store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestFallsBackToPreviousValidRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), manifestName)
	first := testManifestState(1)
	second := testManifestState(2)
	if err := writeManifest(path, first); err != nil {
		t.Fatal(err)
	}
	if err := writeManifest(path, second); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[manifestRecordSize+manifestActiveWALNoOffset] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	got, ok, err := readManifest(path)
	if err != nil || !ok || got != first {
		t.Fatalf("readManifest after corrupt latest = (%+v,%v,%v), want first,true,nil", got, ok, err)
	}
}

func TestManifestCompactsWhenFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), manifestName)
	var want manifestState
	for i := 0; i < manifestSlotCount+2; i++ {
		want = testManifestState(uint64(i + 1))
		if err := writeManifest(path, want); err != nil {
			t.Fatal(err)
		}
	}

	latest, ok, err := readManifestLog(path)
	if err != nil || !ok {
		t.Fatalf("readManifestLog = (%+v,%v,%v), want latest,true,nil", latest, ok, err)
	}
	if latest.slot != 1 {
		t.Fatalf("latest slot after compact and one append = %d, want 1", latest.slot)
	}
	if latest.state != want {
		t.Fatalf("latest state = %+v, want %+v", latest.state, want)
	}
}

func TestManifestObjectWriteUsesCachedLatestRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), manifestName)
	m, _, ok, err := openManifest(path)
	if err != nil || ok {
		t.Fatalf("openManifest absent = ok %v err %v, want false,nil", ok, err)
	}
	defer func() {
		if err := m.close(); err != nil {
			t.Fatal(err)
		}
	}()

	first := testManifestState(1)
	if err := m.write(first); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[manifestCRCOffset] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	second := testManifestState(2)
	if err := m.write(second); err != nil {
		t.Fatal(err)
	}
	latest, ok, err := readManifestLog(path)
	if err != nil || !ok {
		t.Fatalf("readManifestLog = (%+v,%v,%v), want latest,true,nil", latest, ok, err)
	}
	if latest.slot != 1 {
		t.Fatalf("latest slot = %d, want 1", latest.slot)
	}
	if latest.state != second {
		t.Fatalf("latest state = %+v, want %+v", latest.state, second)
	}
}

func TestManifestRejectsNoValidRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), manifestName)
	if err := os.WriteFile(path, make([]byte, manifestSize), 0o600); err != nil {
		t.Fatal(err)
	}

	_, ok, err := readManifest(path)
	if !errors.Is(err, ErrManifest) || ok {
		t.Fatalf("readManifest empty file = (ok=%v, err=%v), want false,%v", ok, err, ErrManifest)
	}
}

func TestManifestAllowsNextFileNoToSkip(t *testing.T) {
	path := filepath.Join(t.TempDir(), manifestName)
	state := testManifestState(1)
	state.nextFileNo = state.activeWALFileNo + 8

	if err := writeManifest(path, state); err != nil {
		t.Fatal(err)
	}
	got, ok, err := readManifest(path)
	if err != nil || !ok || got != state {
		t.Fatalf("readManifest = (%+v,%v,%v), want state,true,nil", got, ok, err)
	}
}

func testManifestState(checkpoint uint64) manifestState {
	return manifestState{
		checkpointWALFileNo: checkpoint,
		activeWALFileNo:     checkpoint + 1,
		nextFileNo:          checkpoint + 2,
		walSegmentSize:      1 << 20,
	}
}
