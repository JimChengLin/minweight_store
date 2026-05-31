//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLoggerRecordsKeyEngineEvents(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store, err := Open(t.TempDir(), Options{
		Logger:        logger,
		WALSize:       crashTestWALSize,
		TargetSSTSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeForTest(t, store)
	stopCompactionDispatchersForTest(store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	sourceWAL := store.checkpointWALFileNo
	compacted, err := store.minorCompactWAL(sourceWAL)
	if err != nil || !compacted {
		t.Fatalf("minorCompactWAL(%d) = (%v,%v), want true,nil", sourceWAL, compacted, err)
	}
	if err := store.flush(); err != nil {
		t.Fatal(err)
	}
	oldSST := onlyParquetFileNoForTest(t, store)
	if err := store.Put([]byte("alpha"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err := store.MajorCompact(); err != nil {
		t.Fatal(err)
	}

	output := logs.String()
	for _, want := range []string{
		"open_start",
		"open_done",
		"flush_start",
		"flush_done",
		"minor_compaction_wal_done",
		"major_compaction_round_done",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("logger output missing %q:\n%s", want, output)
		}
	}
	if !strings.Contains(output, "source_wal_file_no="+strconv.FormatUint(sourceWAL, 10)) {
		t.Fatalf("logger output missing source WAL %d:\n%s", sourceWAL, output)
	}
	if !strings.Contains(output, "old_sst_count=1") || !strings.Contains(output, "live_entry_count=0") {
		t.Fatalf("logger output missing major compaction counts for old SST %d:\n%s", oldSST, output)
	}
}

func TestRotatingLogWriterRotatesAndRetains(t *testing.T) {
	dir := t.TempDir()
	writer, err := openRotatingLogWriter(dir, 24, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := fmt.Fprintf(writer, "entry-%d-xxxxxxxxxxxx\n", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	active, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(active), "entry-4") {
		t.Fatalf("active log = %q, want latest entry", string(active))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var archives []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), logName+".") {
			archives = append(archives, entry.Name())
		}
	}
	if len(archives) != 2 {
		t.Fatalf("rotated archives = %v, want 2 retained archives", archives)
	}
	for _, name := range archives {
		if !strings.Contains(name, "."+strconv.Itoa(os.Getpid())) {
			t.Fatalf("archive name %q does not include pid %d", name, os.Getpid())
		}
	}
}

func TestOpenCreatesDefaultDiskLogger(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, Options{WALSize: crashTestWALSize})
	if err != nil {
		t.Fatal(err)
	}
	stopCompactionDispatchersForTest(store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	output := string(data)
	for _, want := range []string{"open_start", "open_done"} {
		if !strings.Contains(output, want) {
			t.Fatalf("default logger output missing %q:\n%s", want, output)
		}
	}
}
