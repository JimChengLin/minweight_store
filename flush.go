//go:build darwin || linux

package minweight_store

import (
	"os"
	"sync/atomic"

	"github.com/JimChengLin/minpatricia"
)

const (
	primaryIndexName   = "index_primary"
	secondaryIndexName = "index_secondary"
)

func (s *Store) flushWithSecondaryLocked() error {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return err
	}
	active := s.records.activeSegment()
	if active.used == walHeaderSize {
		// Empty WAL is a valid idempotent flush: there is no new checkpoint progress.
		return nil
	}
	checkpointWALFileNo, err := checkpointActiveWAL(
		s.manifest.dir(),
		backend,
		s.records,
		s.manifest,
		s.checkpointWALFileNo,
		WALReplayStrict,
	)
	if err != nil {
		return err
	}
	s.checkpointWALFileNo = checkpointWALFileNo
	return nil
}

func checkpointActiveWAL(dir string, backend *indexBackend, records *segmentedRecordStore, manifest *manifest, checkpointWALFileNo uint64, policy WALReplayPolicy) (uint64, error) {
	oldWAL, err := records.Rollover()
	if err != nil {
		return 0, err
	}
	oldWALFileNo := oldWAL.fileNo
	state := manifestState{
		checkpointWALFileNo: oldWALFileNo,
		activeWALFileNo:     records.activeFileNo,
		nextFileNo:          atomic.LoadUint64(&records.nextFileNo),
		walSegmentSize:      uint64(records.size),
		primaryWALFlushed:   true,
	}
	activeSync := make(chan error, 1)
	go func() {
		activeSync <- records.Sync()
	}()

	if err := syncPrimaryIndexAndWAL(backend, oldWAL); err != nil {
		<-activeSync
		return 0, err
	}
	if err := <-activeSync; err != nil {
		return 0, err
	}
	if err := manifest.write(state); err != nil {
		return 0, err
	}
	state.primaryWALFlushed = false
	if err := checkpointSecondaryIndex(dir, records, oldWALFileNo, policy, backend.index); err != nil {
		return 0, err
	}
	if err := records.deletePendingWALs(); err != nil {
		return 0, err
	}
	if err := manifest.write(state); err != nil {
		return 0, err
	}
	return oldWALFileNo, nil
}

// closeCheckpointWithPrimaryLocked publishes a graceful-shutdown checkpoint
// only when Close has new WAL progress to commit.
//
// Best-effort startup recovery repairs WAL before replay, so Close can publish
// its checkpoint with strict secondary replay like ordinary flush.
func (s *Store) closeCheckpointWithPrimaryLocked() error {
	if s.records == nil && s.manifest == nil {
		return nil
	}

	active := s.records.activeSegment()
	if active.used == walHeaderSize {
		return nil
	}

	checkpointWALFileNo, err := checkpointActiveWAL(
		s.manifest.dir(),
		s.backend,
		s.records,
		s.manifest,
		s.checkpointWALFileNo,
		WALReplayStrict,
	)
	if err != nil {
		return err
	}
	s.checkpointWALFileNo = checkpointWALFileNo
	return nil
}

func syncPrimaryIndexAndWAL(backend *indexBackend, wal *mmapWALRecordStore) error {
	errs := make(chan error, 2)
	go func() {
		errs <- backend.nodes.Sync()
	}()
	go func() {
		errs <- wal.Sync()
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func checkpointSecondaryIndex(dir string, records *segmentedRecordStore, walFileNo uint64, policy WALReplayPolicy, liveIndex *minpatricia.Index) error {
	secondaryPath := secondaryIndexPath(dir)
	if walFileNo == firstWALSegmentNo {
		if err := os.RemoveAll(secondaryPath); err != nil {
			return err
		}
	}
	nodes, err := openMmapNodeStore(secondaryPath)
	if err != nil {
		return err
	}
	closeNodes := true
	defer func() {
		if closeNodes {
			_ = nodes.Close()
		}
	}()

	var index *minpatricia.Index
	if walFileNo == firstWALSegmentNo {
		index = minpatricia.NewWithNodes(records, nodes)
	} else {
		index, err = minpatricia.OpenWithNodes(records, nodes)
		if err != nil {
			return err
		}
	}

	if err := replayWALIntoIndex(records, walFileNo, policy, index, liveIndex); err != nil {
		return err
	}
	if err := nodes.Sync(); err != nil {
		return err
	}
	if err := nodes.closeAfterSync(); err != nil {
		return err
	}
	closeNodes = false
	return nil
}
