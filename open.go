//go:build darwin || linux

package minweight_store

import (
	"os"
	"path/filepath"

	"github.com/JimChengLin/minpatricia"
)

type Options struct {
	WALSize         int64
	WALReplayPolicy WALReplayPolicy
}

func Open(dir string, options ...Options) (*Store, error) {
	cfg := Options{WALSize: defaultWALSize}
	if len(options) != 0 {
		cfg = options[0]
		if cfg.WALSize == 0 {
			cfg.WALSize = defaultWALSize
		}
	}
	if cfg.WALReplayPolicy > WALReplayBestEffort {
		return nil, ErrReplayPolicy
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	manifest := &manifest{path: filepath.Join(dir, manifestName)}

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

	wal, err := openMmapWALRecordStore(filepath.Join(dir, "wal"), cfg.WALSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			_ = wal.Close()
		}
	}()

	manifestWALUsedBytes, clean, err := manifest.read()
	if err != nil {
		return nil, err
	}

	var backend *indexBackend
	if clean && manifestWALUsedBytes == wal.used {
		backend, err = openIndexBackend(wal, nodes)
		if err != nil {
			return nil, err
		}
	} else {
		if err := nodes.Reset(); err != nil {
			return nil, err
		}
		backend = newIndexBackendWithNodes(wal, nodes)
		if err := replayWALIntoIndex(wal, cfg.WALReplayPolicy, backend.index); err != nil {
			return nil, err
		}
	}
	if err := manifest.remove(); err != nil {
		return nil, err
	}

	ok = true
	return &Store{
		backend:  backend,
		manifest: manifest,
		wal:      wal,
	}, nil
}

func replayWALIntoIndex(wal *mmapWALRecordStore, policy WALReplayPolicy, index *minpatricia.Index) error {
	return wal.Replay(policy, func(op byte, key []byte, pos minpatricia.Position) error {
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
