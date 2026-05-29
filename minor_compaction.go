//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/JimChengLin/minpatricia"
)

type walCompactionCandidate struct {
	key []byte
	pos minpatricia.Position
}

func (s *Store) minorCompact() error {
	s.compactionMu.RLock()
	defer s.compactionMu.RUnlock()

	s.primaryMu.RLock()
	if s.backend == nil {
		s.primaryMu.RUnlock()
		return ErrClosed
	}
	if s.fatal != nil {
		err := s.fatal
		s.primaryMu.RUnlock()
		return err
	}
	if s.records == nil && s.manifest == nil {
		s.primaryMu.RUnlock()
		return nil
	}
	if s.records == nil || s.manifest == nil {
		s.primaryMu.RUnlock()
		return ErrManifest
	}
	candidates := s.records.compactableWALFileNos(s.checkpointWALFileNo, s.maxImmutableWALNum)
	s.primaryMu.RUnlock()

	for _, fileNo := range candidates {
		compacted, err := s.minorCompactWAL(fileNo)
		if err != nil {
			return err
		}
		if compacted {
			return nil
		}
	}
	return nil
}

func (s *Store) minorCompactWAL(sourceWALFileNo uint64) (bool, error) {
	candidates, hasKVRecord, err := sortedWALPutCompactionCandidates(s.records, sourceWALFileNo)
	if err != nil {
		return false, err
	}
	if !hasKVRecord {
		return false, nil
	}

	var parquetStore *parquetRecordStore
	parquetOwnedByRecords := false
	defer func() {
		if parquetStore != nil && !parquetOwnedByRecords {
			_ = parquetStore.Abort()
		}
	}()

	for _, candidate := range candidates {
		current, ok, err := s.currentIndexPosition(candidate.key)
		if err != nil {
			return false, err
		}
		if !ok || current != candidate.pos {
			continue
		}
		value, ok := s.records.Value(candidate.pos)
		if !ok {
			return false, ErrRecord
		}
		if parquetStore == nil {
			parquetStore, err = s.records.createParquetSegment()
			if err != nil {
				return false, err
			}
		}
		if _, err := parquetStore.Append(candidate.key, value); err != nil {
			return false, err
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
	if err := s.publishInstalledSST(sourceWALFileNo, parquetStore.fileNo); err != nil {
		return false, err
	}
	return true, nil
}

func sortedWALPutCompactionCandidates(records *segmentedRecordStore, sourceWALFileNo uint64) ([]walCompactionCandidate, bool, error) {
	var candidates []walCompactionCandidate
	hasKVRecord := false
	if err := records.ReplayWAL(sourceWALFileNo, WALReplayStrict, func(op byte, key []byte, pos minpatricia.Position) error {
		if op == walOpPut || op == walOpDelete {
			hasKVRecord = true
		}
		if op == walOpDelete || op == walOpInstallSST {
			return nil
		}
		if op != walOpPut {
			return ErrCorruptWAL
		}
		candidates = append(candidates, walCompactionCandidate{
			key: key,
			pos: pos,
		})
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

	if s.fatal != nil {
		return 0, false, s.fatal
	}
	return s.backend.index.Get(key)
}

func (s *Store) publishInstalledSST(sourceWALFileNo, sstFileNo uint64) error {
	for {
		s.primaryMu.Lock()
		if s.fatal != nil {
			err := s.fatal
			s.primaryMu.Unlock()
			return err
		}
		_, err := s.records.AppendInstallSSTRecord(sourceWALFileNo, sstFileNo)
		walFull := errors.Is(err, ErrWalFull)
		canFlush := false
		if walFull {
			active := s.records.activeSegment()
			canFlush = active != nil && active.used != walHeaderSize
		}
		if err == nil {
			err = installSSTIntoIndex(s.records, s.backend.index, sourceWALFileNo, sstFileNo)
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

func installSSTIntoIndex(records *segmentedRecordStore, index *minpatricia.Index, sourceWALFileNo, sstFileNo uint64) error {
	sst, err := records.parquetSegment(sstFileNo)
	if err != nil {
		return err
	}
	return sst.scanKeys(func(rowIndex uint64, key []byte) error {
		oldPos, ok, err := index.Get(key)
		if err != nil || !ok {
			return err
		}
		if recordPositionFileNo(oldPos) != sourceWALFileNo {
			return nil
		}
		newPos, err := makeParquetRecordPosition(sstFileNo, rowIndex)
		if err != nil {
			return err
		}
		replacedPos, replaced, err := index.Put(key, newPos)
		if err != nil {
			return err
		}
		if !replaced || replacedPos != oldPos {
			return ErrCorruptIndex
		}
		return records.Free(oldPos)
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
