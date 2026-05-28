//go:build linux

package minweight_store

import "os"

func syncMmapFileMetadata(file *os.File) error {
	return nil
}
