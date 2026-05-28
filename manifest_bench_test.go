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
		nextSlot := 1

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if nextSlot >= manifestSlotCount {
				b.StopTimer()
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					b.Fatal(err)
				}
				if err := replaceManifest(path, testManifestState(nextCheckpoint), nextCheckpoint); err != nil {
					b.Fatal(err)
				}
				nextCheckpoint++
				nextSlot = 1
				b.StartTimer()
			}
			if err := writeManifest(path, testManifestState(nextCheckpoint)); err != nil {
				b.Fatal(err)
			}
			nextCheckpoint++
			nextSlot++
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
