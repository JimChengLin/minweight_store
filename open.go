//go:build darwin || linux

package minweight_store

import (
	"os"
	"path/filepath"

	"github.com/JimChengLin/minpatricia"
)

type Options struct {
	WALSize                  int64
	WALReplayPolicy          WALReplayPolicy
	VerifyIndexOnRead        bool
	MinorCompactionThreadNum int
	MajorCompactionThreadNum int
	MaxImmutableWALNum       int
	TargetSSTSize            int64
}

const (
	defaultWALSize                  int64 = 128 * 1024 * 1024
	defaultTargetSSTSize            int64 = 512 * 1024 * 1024
	defaultMinorCompactionThreadNum       = 1
	defaultMajorCompactionThreadNum       = 1
	defaultMaxImmutableWALNum             = 1
)

func Open(dir string, options ...Options) (*Store, error) {
	cfg := defaultOptions()
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
	if cfg.MinorCompactionThreadNum == 0 {
		cfg.MinorCompactionThreadNum = defaultMinorCompactionThreadNum
	}
	if cfg.MajorCompactionThreadNum == 0 {
		cfg.MajorCompactionThreadNum = defaultMajorCompactionThreadNum
	}
	if cfg.MaxImmutableWALNum == 0 {
		cfg.MaxImmutableWALNum = defaultMaxImmutableWALNum
	}
	if cfg.TargetSSTSize == 0 {
		cfg.TargetSSTSize = defaultTargetSSTSize
	}
	if cfg.MinorCompactionThreadNum < 0 || cfg.MajorCompactionThreadNum < 0 || cfg.MaxImmutableWALNum < 0 || cfg.TargetSSTSize < 0 {
		return nil, ErrOptions
	}
	if cfg.WALSize > int64(recordOffsetLimit) {
		return nil, ErrWalFull
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	manifest, state, hasLegalManifest, err := openManifest(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, err
	}
	manifestOwnedByStore := false
	defer func() {
		if !manifestOwnedByStore {
			_ = manifest.close()
		}
	}()

	var opened openedStoreParts
	if hasLegalManifest {
		if !customWALSize {
			cfg.WALSize = int64(state.walSegmentSize)
		}
		if !state.primaryWALFlushed {
			if err := dropEmptyRolloverWAL(walSegmentsPath(dir), cfg.WALSize, state.activeWALFileNo, state.nextFileNo); err != nil {
				return nil, err
			}
		}
		records, err := openSegmentedRecordStore(dir, cfg.WALSize, state.activeWALFileNo, state.nextFileNo)
		if err != nil {
			return nil, err
		}
		opened, err = openFromManifest(dir, manifest, records, state, cfg.WALReplayPolicy)
		if err != nil {
			_ = records.Close()
			return nil, err
		}
		if err := opened.records.cleanupStartupParquetSegments(state.nextFileNo); err != nil {
			_ = opened.backend.close()
			return nil, err
		}
	} else {
		opened, err = rebuildFromWAL(dir, cfg.WALSize, cfg.WALReplayPolicy)
		if err != nil {
			return nil, err
		}
	}
	opened.backend.verifyIndexOnRead = cfg.VerifyIndexOnRead
	store := &Store{
		backend:                  opened.backend,
		manifest:                 manifest,
		records:                  opened.records,
		checkpointWALFileNo:      opened.checkpointWALFileNo,
		minorCompactionThreadNum: cfg.MinorCompactionThreadNum,
		majorCompactionThreadNum: cfg.MajorCompactionThreadNum,
		maxImmutableWALNum:       cfg.MaxImmutableWALNum,
		targetSSTSize:            cfg.TargetSSTSize,
	}
	manifestOwnedByStore = true
	store.startMinorCompactionDispatcher()
	return store, nil
}

func defaultOptions() Options {
	return Options{
		WALSize:                  defaultWALSize,
		MinorCompactionThreadNum: defaultMinorCompactionThreadNum,
		MajorCompactionThreadNum: defaultMajorCompactionThreadNum,
		MaxImmutableWALNum:       defaultMaxImmutableWALNum,
		TargetSSTSize:            defaultTargetSSTSize,
	}
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
	nodesOwnershipTransferred := false
	defer closeMmapNodesUnlessOwnershipTransferred(nodes, &nodesOwnershipTransferred)
	backend, err := openIndexBackend(records, nodes)
	if err != nil {
		return openedStoreParts{}, err
	}
	nodesOwnershipTransferred = true
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
	nodesOwnershipTransferred := false
	defer closeMmapNodesUnlessOwnershipTransferred(nodes, &nodesOwnershipTransferred)
	backend, err := openIndexBackend(records, nodes)
	if err != nil {
		return openedStoreParts{}, err
	}
	if err := replayWALIntoIndex(records, walFileNo, replayPolicy, backend.index, nil); err != nil {
		return openedStoreParts{}, err
	}
	checkpointWALFileNo, err := checkpointActiveWAL(dir, backend, records, manifest, state.checkpointWALFileNo, replayPolicy)
	if err != nil {
		return openedStoreParts{}, err
	}
	nodesOwnershipTransferred = true
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
	nodesOwnershipTransferred := false
	defer closeMmapNodesUnlessOwnershipTransferred(nodes, &nodesOwnershipTransferred)
	backend, err := openIndexBackend(records, nodes)
	if err != nil {
		return openedStoreParts{}, err
	}
	if err := copyMmapNodeStoreDir(primaryIndexPath(dir), secondaryIndexPath(dir)); err != nil {
		return openedStoreParts{}, err
	}
	if err := scheduleDeletesFromInstallRecords(records, state.checkpointWALFileNo); err != nil {
		return openedStoreParts{}, err
	}
	if err := records.deletePendingSegments(); err != nil {
		return openedStoreParts{}, err
	}
	state.primaryWALFlushed = false
	if err := manifest.write(state); err != nil {
		return openedStoreParts{}, err
	}
	nodesOwnershipTransferred = true
	return openedStoreParts{
		backend:             backend,
		records:             records,
		checkpointWALFileNo: state.checkpointWALFileNo,
	}, nil
}

func rebuildFromWAL(dir string, walSize int64, policy WALReplayPolicy) (openedStoreParts, error) {
	walDir := walSegmentsPath(dir)
	if err := createRecordSegmentDirs(dir); err != nil {
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
		// This startup cleanup is idempotent; if the unlink is lost in a crash,
		// the next Open will validate and remove the same empty segment again.
		ids = ids[:1]
	}
	if len(ids) > 1 || len(ids) == 1 && ids[0] != firstWALSegmentNo {
		return openedStoreParts{}, ErrManifest
	}
	var activeWALFileNo, nextFileNo uint64
	if len(ids) == 1 {
		activeWALFileNo = firstWALSegmentNo
		nextFileNo = firstWALSegmentNo + 1
	}
	// Without a manifest commit point, primary is only stale runtime state.
	// The only legal recovery path is rebuilding it from WAL segment 1.
	if err := os.RemoveAll(primaryIndexPath(dir)); err != nil {
		return openedStoreParts{}, err
	}
	records, err := openSegmentedRecordStore(dir, walSize, activeWALFileNo, nextFileNo)
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
	nodesOwnershipTransferred := false
	defer closeMmapNodesUnlessOwnershipTransferred(nodes, &nodesOwnershipTransferred)
	backend := newIndexBackendWithNodes(records, nodes)
	if err := replayWALIntoIndex(records, firstWALSegmentNo, replayPolicy, backend.index, nil); err != nil {
		return openedStoreParts{}, err
	}
	if err := backend.nodes.Sync(); err != nil {
		return openedStoreParts{}, err
	}
	recordsOwnedByStore = true
	nodesOwnershipTransferred = true
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
	// This cleanup is idempotent; durability is not checkpoint progress.
	return nil
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

// liveIndex is the already-synced primary index used by checkpoint replay to
// skip records whose final position is no longer live. Delete has no comparable
// final position, so it is still replayed.
func replayWALIntoIndex(records *segmentedRecordStore, fileNo uint64, policy WALReplayPolicy, index, liveIndex *minpatricia.Index) error {
	return records.ReplayWAL(fileNo, policy, func(op byte, key []byte, pos minpatricia.Position) error {
		switch op {
		case walOpPut:
			if liveIndex != nil {
				livePos, ok, err := liveIndex.Probe(key)
				if err != nil {
					return err
				}
				if !ok || livePos != pos {
					return nil
				}
			}
			_, _, err := index.Put(key, pos)
			return err
		case walOpDelete:
			_, _, err := index.Delete(key)
			return err
		case walOpInstallSST:
			sourceWALFileNo, sstFileNo, err := decodeInstallSSTPayload(key)
			if err != nil {
				return err
			}
			return applyInstallSSTRecord(records, index, liveIndex, sourceWALFileNo, sstFileNo)
		case walOpInstallSSTBatch:
			oldSSTFileNos, newSSTFileNos, err := decodeInstallSSTBatchPayload(key)
			if err != nil {
				return err
			}
			return applyInstallSSTBatchRecord(records, index, liveIndex, oldSSTFileNos, newSSTFileNos)
		default:
			return ErrCorruptWAL
		}
	})
}

func closeMmapNodesUnlessOwnershipTransferred(nodes *mmapNodeStore, ownershipTransferred *bool) {
	if !*ownershipTransferred {
		_ = nodes.Close()
	}
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
