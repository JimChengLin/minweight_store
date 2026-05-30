package minweight_store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkManifestCommit(b *testing.B) {
	b.Run("amortized_write", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), manifestName)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			checkpoint := uint64(i + 1)
			if err := writeManifest(path, testManifestState(checkpoint)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("append_slot", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), manifestName)
		nextCheckpoint := uint64(1)
		if err := replaceManifest(path, testManifestState(nextCheckpoint), nextCheckpoint); err != nil {
			b.Fatal(err)
		}
		nextCheckpoint++
		nextOffset := manifestRecordHeaderSize

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if nextOffset+manifestRecordHeaderSize > manifestSize {
				b.StopTimer()
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					b.Fatal(err)
				}
				if err := replaceManifest(path, testManifestState(nextCheckpoint), nextCheckpoint); err != nil {
					b.Fatal(err)
				}
				nextCheckpoint++
				nextOffset = manifestRecordHeaderSize
				b.StartTimer()
			}
			if err := writeManifest(path, testManifestState(nextCheckpoint)); err != nil {
				b.Fatal(err)
			}
			nextCheckpoint++
			nextOffset += manifestRecordHeaderSize
		}
	})

	b.Run("transient_cached_append_slot", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), manifestName)
		nextCheckpoint := uint64(1)
		nextSeq := uint64(1)
		if err := replaceManifest(path, testManifestState(nextCheckpoint), nextSeq); err != nil {
			b.Fatal(err)
		}
		nextCheckpoint++
		nextSeq++
		nextOffset := manifestRecordHeaderSize

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if nextOffset+manifestRecordHeaderSize > manifestSize {
				b.StopTimer()
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					b.Fatal(err)
				}
				if err := replaceManifest(path, testManifestState(nextCheckpoint), nextSeq); err != nil {
					b.Fatal(err)
				}
				nextCheckpoint++
				nextSeq++
				nextOffset = manifestRecordHeaderSize
				b.StartTimer()
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			record, err := encodeManifestRecord(testManifestState(nextCheckpoint), nextSeq)
			if err != nil {
				b.Fatal(err)
			}
			firstErr := appendManifestRecord(file, record, nextOffset)
			if err := file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			nextCheckpoint++
			nextSeq++
			nextOffset += manifestRecordHeaderSize
		}
	})

	b.Run("persistent_append_slot", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), manifestName)
		nextCheckpoint := uint64(1)
		m, _, _, err := openManifest(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := m.close(); err != nil {
				b.Fatal(err)
			}
		})
		if err := m.write(testManifestState(nextCheckpoint)); err != nil {
			b.Fatal(err)
		}
		nextCheckpoint++

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if m.nextOffset+manifestRecordHeaderSize > manifestSize {
				b.StopTimer()
				if err := m.close(); err != nil {
					b.Fatal(err)
				}
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					b.Fatal(err)
				}
				m, _, _, err = openManifest(path)
				if err != nil {
					b.Fatal(err)
				}
				if err := m.write(testManifestState(nextCheckpoint)); err != nil {
					b.Fatal(err)
				}
				nextCheckpoint++
				b.StartTimer()
			}
			if err := m.write(testManifestState(nextCheckpoint)); err != nil {
				b.Fatal(err)
			}
			nextCheckpoint++
		}
	})

	b.Run("replace_manifest", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), manifestName)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			checkpoint := uint64(i + 1)
			if err := replaceManifest(path, testManifestState(checkpoint), checkpoint); err != nil {
				b.Fatal(err)
			}
		}
	})
}
