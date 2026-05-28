//go:build darwin || linux

package minweight_store

import (
	"errors"
	"io"
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
	customWALSize := false
	if len(options) != 0 {
		cfg = options[0]
		if cfg.WALSize == 0 {
			cfg.WALSize = defaultWALSize
		} else {
			customWALSize = true
		}
	}
	if cfg.WALReplayPolicy > WALReplayBestEffort {
		return nil, ErrReplayPolicy
	}
	if cfg.WALSize > int64(recordOffsetLimit) {
		return nil, ErrWalFull
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	manifest := &manifest{path: filepath.Join(dir, manifestName)}

	state, hasLegalManifest, err := manifest.read()
	if err != nil {
		return nil, err
	}
	var opened openedStoreParts
	if hasLegalManifest {
		if !customWALSize {
			cfg.WALSize = int64(state.walSegmentSize)
		}
		if !state.primaryWALFlushed {
			if err := dropEmptyRolloverWAL(walSegmentsPath(dir), cfg.WALSize, state.activeWALFileNo, state.nextWALFileNo); err != nil {
				return nil, err
			}
		}
		records, err := openSegmentedRecordStore(walSegmentsPath(dir), cfg.WALSize, state.activeWALFileNo, state.nextWALFileNo)
		if err != nil {
			return nil, err
		}
		opened, err = openFromManifest(dir, manifest, records, state, cfg.WALReplayPolicy)
		if err != nil {
			_ = records.Close()
			return nil, err
		}
	} else {
		opened, err = rebuildFromWAL(dir, cfg.WALSize, cfg.WALReplayPolicy)
		if err != nil {
			return nil, err
		}
	}
	return &Store{
		backend:             opened.backend,
		manifest:            manifest,
		records:             opened.records,
		checkpointWALFileNo: opened.checkpointWALFileNo,
	}, nil
}

type openedStoreParts struct {
	backend             *indexBackend
	records             *segmentedRecordStore
	checkpointWALFileNo uint64
}

func openFromManifest(dir string, manifest *manifest, records *segmentedRecordStore, state manifestState, policy WALReplayPolicy) (openedStoreParts, error) {
	if state.primaryWALFlushed {
		return recoverPrimaryFlushedCheckpoint(dir, manifest, records, state)
	}
	active := records.activeSegment()
	if active.used == walHeaderSize {
		return openCleanManifest(dir, records, state.checkpointWALFileNo)
	}
	return recoverManifestTail(dir, manifest, records, state, policy)
}

func openCleanManifest(dir string, records *segmentedRecordStore, checkpointWALFileNo uint64) (openedStoreParts, error) {
	if err := requireMmapNodeStoreDir(primaryIndexPath(dir)); err != nil {
		return openedStoreParts{}, err
	}
	nodes, err := openMmapNodeStore(primaryIndexPath(dir))
	if err != nil {
		return openedStoreParts{}, err
	}
	backend, err := openIndexBackend(records, nodes)
	if err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	return openedStoreParts{
		backend:             backend,
		records:             records,
		checkpointWALFileNo: checkpointWALFileNo,
	}, nil
}

func recoverManifestTail(dir string, manifest *manifest, records *segmentedRecordStore, state manifestState, policy WALReplayPolicy) (openedStoreParts, error) {
	walFileNo := records.activeFileNo
	replayPolicy, err := prepareWALForReplay(records, walFileNo, policy)
	if err != nil {
		return openedStoreParts{}, err
	}
	if err := copyMmapNodeStoreDir(secondaryIndexPath(dir), primaryIndexPath(dir)); err != nil {
		return openedStoreParts{}, err
	}
	nodes, err := openMmapNodeStore(primaryIndexPath(dir))
	if err != nil {
		return openedStoreParts{}, err
	}
	backend, err := openIndexBackend(records, nodes)
	if err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	if err := replayWALIntoIndex(records, walFileNo, replayPolicy, backend.index); err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	checkpointWALFileNo, err := checkpointActiveWAL(dir, backend, records, manifest, state.checkpointWALFileNo, replayPolicy)
	if err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	return openedStoreParts{
		backend:             backend,
		records:             records,
		checkpointWALFileNo: checkpointWALFileNo,
	}, nil
}

func recoverPrimaryFlushedCheckpoint(dir string, manifest *manifest, records *segmentedRecordStore, state manifestState) (openedStoreParts, error) {
	active := records.activeSegment()
	if active.used != walHeaderSize {
		return openedStoreParts{}, ErrManifest
	}
	if err := requireMmapNodeStoreDir(primaryIndexPath(dir)); err != nil {
		return openedStoreParts{}, err
	}
	nodes, err := openMmapNodeStore(primaryIndexPath(dir))
	if err != nil {
		return openedStoreParts{}, err
	}
	backend, err := openIndexBackend(records, nodes)
	if err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	if err := copyMmapNodeStoreDir(primaryIndexPath(dir), secondaryIndexPath(dir)); err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	state.primaryWALFlushed = false
	if err := manifest.write(state); err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	return openedStoreParts{
		backend:             backend,
		records:             records,
		checkpointWALFileNo: state.checkpointWALFileNo,
	}, nil
}

func rebuildFromWAL(dir string, walSize int64, policy WALReplayPolicy) (openedStoreParts, error) {
	walDir := walSegmentsPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return openedStoreParts{}, err
	}
	ids, err := listWALSegmentIDs(walDir)
	if err != nil {
		return openedStoreParts{}, err
	}
	if len(ids) == 2 && ids[0] == firstWALSegmentNo && ids[1] == firstWALSegmentNo+1 {
		empty, err := walSegmentEmpty(walDir, walSize, ids[1])
		if err != nil {
			return openedStoreParts{}, err
		}
		if !empty {
			return openedStoreParts{}, ErrManifest
		}
		if err := os.Remove(filepath.Join(walDir, walSegmentName(ids[1]))); err != nil {
			return openedStoreParts{}, err
		}
		if err := syncDir(walDir); err != nil {
			return openedStoreParts{}, err
		}
		ids = ids[:1]
	}
	if len(ids) > 1 || len(ids) == 1 && ids[0] != firstWALSegmentNo {
		return openedStoreParts{}, ErrManifest
	}
	var activeWALFileNo, nextWALFileNo uint64
	if len(ids) == 1 {
		activeWALFileNo = firstWALSegmentNo
		nextWALFileNo = firstWALSegmentNo + 1
	}
	// Without a manifest commit point, primary is only stale runtime state.
	// The only legal recovery path is rebuilding it from WAL segment 1.
	if err := os.RemoveAll(primaryIndexPath(dir)); err != nil {
		return openedStoreParts{}, err
	}
	records, err := openSegmentedRecordStore(walDir, walSize, activeWALFileNo, nextWALFileNo)
	if err != nil {
		return openedStoreParts{}, err
	}
	recordsOwnedByStore := false
	defer func() {
		if !recordsOwnedByStore {
			_ = records.Close()
		}
	}()

	if records.activeFileNo != firstWALSegmentNo {
		return openedStoreParts{}, ErrManifest
	}
	replayPolicy, err := prepareWALForReplay(records, firstWALSegmentNo, policy)
	if err != nil {
		return openedStoreParts{}, err
	}
	nodes, err := openMmapNodeStore(primaryIndexPath(dir))
	if err != nil {
		return openedStoreParts{}, err
	}
	backend := newIndexBackendWithNodes(records, nodes)
	if err := replayWALIntoIndex(records, firstWALSegmentNo, replayPolicy, backend.index); err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	if err := backend.nodes.Sync(); err != nil {
		_ = nodes.Close()
		return openedStoreParts{}, err
	}
	recordsOwnedByStore = true
	return openedStoreParts{
		backend: backend,
		records: records,
	}, nil
}

func walSegmentEmpty(dir string, walSize int64, fileNo uint64) (bool, error) {
	wal, err := openMmapWALRecordStore(filepath.Join(dir, walSegmentName(fileNo)), walSize, fileNo)
	if err != nil {
		return false, err
	}
	defer wal.Close()
	return wal.used == walHeaderSize, nil
}

func dropEmptyRolloverWAL(dir string, walSize int64, activeFileNo, nextFileNo uint64) error {
	ids, err := listWALSegmentIDs(dir)
	if err != nil {
		return err
	}
	if len(ids) < 2 || ids[len(ids)-2] != activeFileNo || ids[len(ids)-1] != nextFileNo {
		return nil
	}
	empty, err := walSegmentEmpty(dir, walSize, nextFileNo)
	if err != nil {
		return err
	}
	if !empty {
		return ErrManifest
	}
	if err := os.Remove(filepath.Join(dir, walSegmentName(nextFileNo))); err != nil {
		return err
	}
	return syncDir(dir)
}

func prepareWALForReplay(records *segmentedRecordStore, fileNo uint64, policy WALReplayPolicy) (WALReplayPolicy, error) {
	if policy != WALReplayBestEffort {
		return policy, nil
	}
	if _, err := records.repairWALBestEffort(fileNo); err != nil {
		return 0, err
	}
	return WALReplayStrict, nil
}

func replayWALIntoIndex(records *segmentedRecordStore, fileNo uint64, policy WALReplayPolicy, index *minpatricia.Index) error {
	return records.ReplayWAL(fileNo, policy, func(op byte, key []byte, pos minpatricia.Position) error {
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

func primaryIndexPath(dir string) string {
	return filepath.Join(dir, primaryIndexName)
}

func secondaryIndexPath(dir string) string {
	return filepath.Join(dir, secondaryIndexName)
}

func walSegmentsPath(dir string) string {
	return filepath.Join(dir, walDirName)
}

func copyMmapNodeStoreDir(src, dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return ErrManifest
		}
		if err := copyFile(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
			return err
		}
	}
	return syncDir(dst)
}

func requireMmapNodeStoreDir(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrManifest
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return ErrManifest
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	copyComplete := false
	defer func() {
		if !copyComplete {
			_ = out.Close()
			_ = os.Remove(dst)
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	copyComplete = true
	return nil
}
