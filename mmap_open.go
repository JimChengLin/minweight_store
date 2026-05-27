//go:build darwin || linux

package minweight_store

import (
	"os"
	"path/filepath"

	"github.com/JimChengLin/minpatricia"
)

type Options struct {
	WALSize int64
}

func Open(dir string, options ...Options) (*Store, error) {
	cfg := Options{WALSize: defaultWALSize}
	if len(options) != 0 {
		cfg = options[0]
		if cfg.WALSize == 0 {
			cfg.WALSize = defaultWALSize
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	nodes, err := openMmapNodeStore(filepath.Join(dir, "index"))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = nodes.Close()
		}
	}()
	if err := nodes.Reset(); err != nil {
		return nil, err
	}

	wal, err := openWALRecordStore(filepath.Join(dir, "wal"), cfg.WALSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			_ = wal.Close()
		}
	}()

	backend := newIndexBackendWithNodes(wal, nodes)
	if err := replayWALIntoIndex(wal, backend.index); err != nil {
		return nil, err
	}

	ok = true
	return &Store{
		backend: backend,
	}, nil
}

func replayWALIntoIndex(wal *walRecordStore, index *minpatricia.Index) error {
	return wal.Replay(func(op byte, key []byte, pos minpatricia.Position) error {
		switch op {
		case walOpPut:
			_, _, err := index.Put(key, pos)
			return err
		case walOpDelete:
			_, _, err := index.Delete(key)
			return err
		default:
			return ErrCorruptWAL
		}
	})
}
