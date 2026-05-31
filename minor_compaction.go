//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/JimChengLin/minpatricia"
)

type walCompactionCandidate struct {
	key []byte
	pos minpatricia.Position
}

func (s *Store) minorCompact() error {
	s.compactionMu.RLock()
	defer s.compactionMu.RUnlock()

	candidates, err := s.minorCompactionCandidates()
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	start := time.Now()
	logInfo(s.logger, "minor_compaction_start",
		"candidate_wal_count", len(candidates),
		"workers", s.minorCompactionThreadNum,
	)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	hasErr := func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr != nil
	}
	sem := make(chan struct{}, s.minorCompactionThreadNum)
	for _, fileNo := range candidates {
		if hasErr() {
			break
		}
		sem <- struct{}{}

		wg.Add(1)
		go func(fileNo uint64) {
			defer wg.Done()
			defer func() {
				<-sem
			}()

			_, err := s.minorCompactWAL(fileNo)
			if err == nil {
				return
			}
			setErr(err)
		}(fileNo)
	}
	wg.Wait()
	logInfo(s.logger, "minor_compaction_done",
		"candidate_wal_count", len(candidates),
		"duration", time.Since(start),
	)
	return firstErr
}

func (s *Store) minorCompactionCandidates() ([]uint64, error) {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	if _, err := s.openBackend(); err != nil {
		return nil, err
	}
	if s.records == nil && s.manifest == nil {
		return nil, nil
	}
	if s.records == nil || s.manifest == nil {
		return nil, ErrManifest
	}
	return s.records.compactableWALFileNos(s.checkpointWALFileNo, s.maxImmutableWALNum), nil
}

func (s *Store) minorCompactWAL(sourceWALFileNo uint64) (bool, error) {
	start := time.Now()
	candidates, hasKVRecord, err := sortedWALPutCompactionCandidates(s.records, sourceWALFileNo)
	if err != nil {
		return false, err
	}
	if !hasKVRecord {
		logInfo(s.logger, "minor_compaction_wal_skip",
			"source_wal_file_no", sourceWALFileNo,
			"reason", "no_kv_record",
			"duration", time.Since(start),
		)
		return false, nil
	}

	var parquetStore *parquetRecordStore
	parquetOwnedByRecords := false
	defer func() {
		if parquetStore != nil && !parquetOwnedByRecords {
			_ = parquetStore.Abort()
		}
	}()

	liveCandidates := candidates[:0]
	for _, candidate := range candidates {
		current, ok, err := s.currentIndexPosition(candidate.key)
		if err != nil {
			return false, err
		}
		if !ok || current != candidate.pos {
			continue
		}
		liveCandidates = append(liveCandidates, candidate)
	}

	for i, candidate := range liveCandidates {
		if parquetStore == nil {
			parquetStore, err = s.records.createParquetSegment()
			if err != nil {
				return false, err
			}
		}
		value, ok := s.records.Value(candidate.pos)
		if !ok {
			return false, ErrCorruptWAL
		}
		newPos, err := parquetStore.Append(candidate.key, value)
		if err != nil {
			return false, err
		}
		fileNo, rowIndex, ok := parseParquetRecordPosition(newPos)
		if !ok || fileNo != parquetStore.fileNo || rowIndex != uint64(i) {
			return false, ErrParquet
		}
	}
	if parquetStore == nil {
		parquetStore, err = s.records.createParquetSegment()
		if err != nil {
			return false, err
		}
	}
	if err := parquetStore.Sync(); err != nil {
		return false, err
	}
	if err := s.records.installParquetSegment(parquetStore); err != nil {
		return false, err
	}
	parquetOwnedByRecords = true
	if err := s.publishInstalledSST(sourceWALFileNo, parquetStore.fileNo, liveCandidates); err != nil {
		return false, err
	}
	logInfo(s.logger, "minor_compaction_wal_done",
		"source_wal_file_no", sourceWALFileNo,
		"sst_file_no", parquetStore.fileNo,
		"put_candidate_count", len(candidates),
		"live_candidate_count", len(liveCandidates),
		"duration", time.Since(start),
	)
	return true, nil
}

func sortedWALPutCompactionCandidates(records *segmentedRecordStore, sourceWALFileNo uint64) ([]walCompactionCandidate, bool, error) {
	var candidates []walCompactionCandidate
	hasKVRecord := false
	if err := records.ReplayWAL(sourceWALFileNo, WALReplayStrict, func(op byte, key []byte, pos minpatricia.Position) error {
		if op == walOpPut || op == walOpDelete {
			hasKVRecord = true
		}
		switch op {
		case walOpPut:
			candidates = append(candidates, walCompactionCandidate{
				key: key,
				pos: pos,
			})
		case walOpDelete, walOpInstallSST, walOpInstallSSTBatch:
			return nil
		default:
			return ErrCorruptWAL
		}
		return nil
	}); err != nil {
		return nil, false, err
	}
	sort.Slice(candidates, func(i, j int) bool {
		return bytes.Compare(candidates[i].key, candidates[j].key) < 0
	})
	return candidates, hasKVRecord, nil
}

func (s *Store) currentIndexPosition(key []byte) (minpatricia.Position, bool, error) {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	backend, err := s.openBackend()
	if err != nil {
		return 0, false, err
	}
	return backend.index.Probe(key)
}

func (s *Store) publishInstalledSST(sourceWALFileNo, sstFileNo uint64, entries []walCompactionCandidate) error {
	for {
		s.primaryMu.Lock()
		backend, err := s.openBackend()
		if err != nil {
			s.primaryMu.Unlock()
			return err
		}
		_, err = s.records.AppendInstallSSTRecord(sourceWALFileNo, sstFileNo)
		walFull := errors.Is(err, ErrWalFull)
		canFlush := false
		if walFull {
			active := s.records.activeSegment()
			canFlush = active != nil && active.used != walHeaderSize
		}
		if err == nil {
			err = s.records.markSSTLive(sstFileNo)
		}
		if err == nil {
			err = retargetInstalledSSTEntries(s.records, backend.index, sstFileNo, entries)
		}
		if err == nil {
			err = s.records.scheduleWALDelete(sourceWALFileNo)
		}
		if err != nil && !walFull {
			s.fatal = errors.Join(ErrFatal, err)
			err = s.fatal
		}
		s.primaryMu.Unlock()

		if walFull {
			if !canFlush {
				return ErrWalFull
			}
			if err := s.flush(); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

func retargetInstalledSSTEntries(records *segmentedRecordStore, index *minpatricia.Index, sstFileNo uint64, entries []walCompactionCandidate) error {
	for rowIndex, entry := range entries {
		newPos, err := makeParquetRecordPosition(sstFileNo, uint64(rowIndex))
		if err != nil {
			return err
		}
		oldPos, ok, err := index.Probe(entry.key)
		if err != nil {
			return err
		}
		if !ok || oldPos != entry.pos {
			if err := records.Free(newPos); err != nil {
				return err
			}
			continue
		}
		if err := retargetIndexPosition(index, entry.key, oldPos, newPos); err != nil {
			return err
		}
		if err := records.Free(oldPos); err != nil {
			return err
		}
	}
	return nil
}

func applyInstallSSTRecord(records *segmentedRecordStore, index, liveIndex *minpatricia.Index, sourceWALFileNo, sstFileNo uint64) error {
	sst, err := records.parquetSegment(sstFileNo)
	if err != nil {
		return err
	}
	if err := records.markSSTLive(sstFileNo); err != nil {
		return err
	}
	if err := sst.scanKeys(func(rowIndex uint64, key []byte) error {
		newPos, err := makeParquetRecordPosition(sstFileNo, rowIndex)
		if err != nil {
			return err
		}
		if liveIndex != nil {
			livePos, ok, err := liveIndex.Probe(key)
			if err != nil {
				return err
			}
			if !ok || livePos != newPos {
				return nil
			}
			oldPos, ok, err := index.Probe(key)
			if err != nil {
				return err
			}
			if !ok || recordPositionFileNo(oldPos) != sourceWALFileNo {
				return nil
			}
			return retargetIndexPosition(index, key, oldPos, newPos)
		}
		oldPos, ok, err := index.Get(key)
		if err != nil {
			return err
		}
		if !ok || recordPositionFileNo(oldPos) != sourceWALFileNo {
			return records.Free(newPos)
		}
		if err := retargetIndexPosition(index, key, oldPos, newPos); err != nil {
			return err
		}
		return records.Free(oldPos)
	}); err != nil {
		return err
	}
	return records.scheduleWALDelete(sourceWALFileNo)
}

func scheduleDeletesFromInstallRecords(records *segmentedRecordStore, fileNo uint64) error {
	return records.ReplayWAL(fileNo, WALReplayStrict, func(op byte, key []byte, pos minpatricia.Position) error {
		switch op {
		case walOpInstallSST:
			sourceWALFileNo, _, err := decodeInstallSSTPayload(key)
			if err != nil {
				return err
			}
			return records.scheduleWALDeleteIfPresent(sourceWALFileNo)
		case walOpInstallSSTBatch:
			oldSSTFileNos, _, err := decodeInstallSSTBatchPayload(key)
			if err != nil {
				return err
			}
			for _, fileNo := range oldSSTFileNos {
				if err := records.scheduleSSTDeleteIfPresent(fileNo); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func decodeInstallSSTPayload(payload []byte) (uint64, uint64, error) {
	if len(payload) != walInstallSSTPayloadSize {
		return 0, 0, ErrCorruptWAL
	}
	sourceWALFileNo := binary.LittleEndian.Uint64(payload[:8])
	sstFileNo := binary.LittleEndian.Uint64(payload[8:])
	if sourceWALFileNo == 0 || sstFileNo == 0 {
		return 0, 0, ErrCorruptWAL
	}
	return sourceWALFileNo, sstFileNo, nil
}
