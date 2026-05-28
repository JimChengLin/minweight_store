//go:build darwin

package minweight_store

import "os"

func syncMmapFileMetadata(file *os.File) error {
	return file.Sync()
}
