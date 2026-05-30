//go:build darwin || linux

package minweight_store

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/JimChengLin/minpatricia"
)

type majorCompactionEntry struct {
	key    []byte
	oldPos minpatricia.Position
	newPos minpatricia.Position
}

func (s *Store) MajorCompact() error {
	s.compactionMu.Lock()
	defer s.compactionMu.Unlock()

	oldSSTFileNos, err := s.majorCompactionSSTFileNos()
	if err != nil || len(oldSSTFileNos) == 0 {
		return err
	}

	newSSTs, liveEntries, err := s.buildMajorCompactionSSTs(oldSSTFileNos)
	newSSTsOwnedByRecords := false
	defer func() {
		if !newSSTsOwnedByRecords {
			_ = cleanupMajorCompactionSSTs(newSSTs)
		}
	}()
	if err != nil {
		return err
	}
	if err := s.records.installParquetSegments(newSSTs); err != nil {
		return err
	}
	newSSTsOwnedByRecords = true
	return s.publishInstalledSSTBatch(oldSSTFileNos, newSSTs, liveEntries)
}

func (s *Store) majorCompactionSSTFileNos() ([]uint64, error) {
	s.primaryMu.RLock()
	defer s.primaryMu.RUnlock()

	_, err := s.openBackend()
	if err != nil {
		return nil, err
	}
	if s.records == nil && s.manifest == nil {
		return nil, nil
	}
	if s.records == nil || s.manifest == nil {
		return nil, ErrManifest
	}
	return s.records.compactableParquetFileNos(), nil
}

type majorCompactionKeyStream struct {
	fileNo uint64
	reader *parquetRecordKeyReader
	entry  majorCompactionEntry
}

type majorCompactionKeyStreamHeap []*majorCompactionKeyStream

func (h *majorCompactionKeyStreamHeap) Len() int {
	return len(*h)
}

func (h *majorCompactionKeyStreamHeap) Less(i, j int) bool {
	left := (*h)[i].entry
	right := (*h)[j].entry
	cmp := bytes.Compare(left.key, right.key)
	if cmp != 0 {
		return cmp < 0
	}
	return left.oldPos < right.oldPos
}

func (h *majorCompactionKeyStreamHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *majorCompactionKeyStreamHeap) Push(x any) {
	*h = append(*h, x.(*majorCompactionKeyStream))
}

func (h *majorCompactionKeyStreamHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	*h = old[:n-1]
	return entry
}

func (s *Store) majorCompactionKeyStreams(fileNos []uint64) (majorCompactionKeyStreamHeap, []*majorCompactionKeyStream, error) {
	streams := make(majorCompactionKeyStreamHeap, 0, len(fileNos))
	allStreams := make([]*majorCompactionKeyStream, 0, len(fileNos))
	for _, fileNo := range fileNos {
		sst, err := s.records.parquetSegment(fileNo)
		if err != nil {
			_ = closeMajorCompactionKeyStreams(allStreams)
			return nil, nil, err
		}
		stream := &majorCompactionKeyStream{
			fileNo: fileNo,
			reader: sst.newKeyReader(),
		}
		allStreams = append(allStreams, stream)
		ok, err := stream.advance()
		if err != nil {
			_ = closeMajorCompactionKeyStreams(allStreams)
			return nil, nil, err
		}
		if ok {
			streams = append(streams, stream)
		}
	}
	heap.Init(&streams)
	return streams, allStreams, nil
}

func (s *majorCompactionKeyStream) advance() (bool, error) {
	rowIndex, key, ok, err := s.reader.next()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	oldPos, err := makeParquetRecordPosition(s.fileNo, rowIndex)
	if err != nil {
		return false, err
	}
	s.entry = majorCompactionEntry{
		key:    key,
		oldPos: oldPos,
	}
	return true, nil
}

func closeMajorCompactionKeyStreams(streams []*majorCompactionKeyStream) error {
	var firstErr error
	for _, stream := range streams {
		if err := stream.reader.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Store) buildMajorCompactionSSTs(oldSSTFileNos []uint64) ([]*parquetRecordStore, []majorCompactionEntry, error) {
	var stores []*parquetRecordStore
	var current *parquetRecordStore
	var currentBytes int64
	var currentRows uint64
	var liveEntries []majorCompactionEntry
	streams, allStreams, err := s.majorCompactionKeyStreams(oldSSTFileNos)
	if err != nil {
		return stores, nil, err
	}
	defer func() {
		_ = closeMajorCompactionKeyStreams(allStreams)
	}()
	buildOwnedByCaller := false
	defer func() {
		if !buildOwnedByCaller && current != nil {
			_ = current.Abort()
		}
	}()

	finishCurrent := func() error {
		if current == nil {
			return nil
		}
		if err := current.Sync(); err != nil {
			return err
		}
		stores = append(stores, current)
		current = nil
		currentBytes = 0
		currentRows = 0
		return nil
	}

	for streams.Len() != 0 {
		stream := heap.Pop(&streams).(*majorCompactionKeyStream)
		entry := stream.entry
		currentPos, ok, err := s.currentIndexPosition(entry.key)
		if err != nil {
			return stores, nil, err
		}
		if ok && currentPos == entry.oldPos {
			value, ok := s.records.Value(entry.oldPos)
			if !ok {
				return stores, nil, ErrCorruptIndex
			}
			entryBytes := int64(len(entry.key) + len(value))
			if current != nil && currentRows != 0 && currentBytes+entryBytes > s.targetSSTSize {
				if err := finishCurrent(); err != nil {
					return stores, nil, err
				}
			}
			if current == nil {
				current, err = s.records.createParquetSegment()
				if err != nil {
					return stores, nil, err
				}
			}
			newPos, err := current.Append(entry.key, value)
			if err != nil {
				return stores, nil, err
			}
			entry.key = cloneBytes(entry.key)
			entry.newPos = newPos
			liveEntries = append(liveEntries, entry)
			currentBytes += entryBytes
			currentRows++
		}

		ok, err = stream.advance()
		if err != nil {
			return stores, nil, err
		}
		if ok {
			heap.Push(&streams, stream)
		}
	}
	if err := finishCurrent(); err != nil {
		return stores, nil, err
	}
	buildOwnedByCaller = true
	return stores, liveEntries, nil
}

func (s *Store) publishInstalledSSTBatch(oldSSTFileNos []uint64, newSSTs []*parquetRecordStore, entries []majorCompactionEntry) error {
	newSSTFileNos := parquetStoreFileNos(newSSTs)

	for {
		s.primaryMu.Lock()
		backend, err := s.openBackend()
		if err != nil {
			s.primaryMu.Unlock()
			return err
		}
		_, err = s.records.AppendInstallSSTBatchRecord(oldSSTFileNos, newSSTFileNos)
		walFull := errors.Is(err, ErrWalFull)
		canFlush := false
		if walFull {
			active := s.records.activeSegment()
			canFlush = active != nil && active.used != walHeaderSize
		}
		if err == nil {
			err = retargetMajorSSTEntries(s.records, backend.index, entries)
		}
		if err == nil {
			for _, fileNo := range oldSSTFileNos {
				if err = s.records.scheduleSSTDelete(fileNo); err != nil {
					break
				}
			}
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

func retargetMajorSSTEntries(records *segmentedRecordStore, index *minpatricia.Index, entries []majorCompactionEntry) error {
	for _, entry := range entries {
		oldPos, ok, err := index.Probe(entry.key)
		if err != nil {
			return err
		}
		if !ok || oldPos != entry.oldPos {
			continue
		}
		if err := retargetIndexPosition(index, entry.key, oldPos, entry.newPos); err != nil {
			return err
		}
		if err := records.Free(oldPos); err != nil {
			return err
		}
	}
	return nil
}

func applyInstallSSTBatchRecord(records *segmentedRecordStore, index, liveIndex *minpatricia.Index, oldSSTFileNos, newSSTFileNos []uint64) error {
	oldSSTs := recordFileNoSet(oldSSTFileNos)
	for _, fileNo := range newSSTFileNos {
		if _, ok := oldSSTs[fileNo]; ok {
			return ErrCorruptWAL
		}
	}

	for _, sstFileNo := range newSSTFileNos {
		sst, err := records.parquetSegment(sstFileNo)
		if err != nil {
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
			}
			oldPos, ok, err := index.Get(key)
			if err != nil || !ok {
				return err
			}
			if _, ok := oldSSTs[recordPositionFileNo(oldPos)]; !ok {
				return nil
			}
			if err := retargetIndexPosition(index, key, oldPos, newPos); err != nil {
				return err
			}
			return records.Free(oldPos)
		}); err != nil {
			return err
		}
	}
	for _, fileNo := range oldSSTFileNos {
		if err := records.scheduleSSTDelete(fileNo); err != nil {
			return err
		}
	}
	return nil
}

func encodeInstallSSTBatchPayload(oldSSTFileNos, newSSTFileNos []uint64) ([]byte, error) {
	if len(oldSSTFileNos) == 0 {
		return nil, ErrCorruptWAL
	}
	totalFileNos := len(oldSSTFileNos) + len(newSSTFileNos)
	if totalFileNos > int(^uint32(0)) {
		return nil, ErrWalFull
	}
	payload := make([]byte, walInstallSSTBatchHeaderSize+totalFileNos*8)
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(oldSSTFileNos)))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(newSSTFileNos)))
	offset := walInstallSSTBatchHeaderSize
	for _, fileNo := range oldSSTFileNos {
		if !validRecordFileNo(fileNo) {
			return nil, minpatricia.ErrPositionTag
		}
		binary.LittleEndian.PutUint64(payload[offset:offset+8], fileNo)
		offset += 8
	}
	for _, fileNo := range newSSTFileNos {
		if !validRecordFileNo(fileNo) {
			return nil, minpatricia.ErrPositionTag
		}
		binary.LittleEndian.PutUint64(payload[offset:offset+8], fileNo)
		offset += 8
	}
	return payload, nil
}

func decodeInstallSSTBatchPayload(payload []byte) ([]uint64, []uint64, error) {
	oldCount, newCount, err := validateInstallSSTBatchPayload(payload)
	if err != nil {
		return nil, nil, err
	}

	oldSSTFileNos := make([]uint64, oldCount)
	newSSTFileNos := make([]uint64, newCount)
	offset := walInstallSSTBatchHeaderSize
	for i := range oldSSTFileNos {
		fileNo := binary.LittleEndian.Uint64(payload[offset : offset+8])
		oldSSTFileNos[i] = fileNo
		offset += 8
	}
	for i := range newSSTFileNos {
		fileNo := binary.LittleEndian.Uint64(payload[offset : offset+8])
		newSSTFileNos[i] = fileNo
		offset += 8
	}
	return oldSSTFileNos, newSSTFileNos, nil
}

func validateInstallSSTBatchPayload(payload []byte) (int, int, error) {
	if len(payload) < walInstallSSTBatchHeaderSize {
		return 0, 0, ErrCorruptWAL
	}
	oldCount := int(binary.LittleEndian.Uint32(payload[:4]))
	newCount := int(binary.LittleEndian.Uint32(payload[4:8]))
	if oldCount == 0 {
		return 0, 0, ErrCorruptWAL
	}
	if len(payload) != walInstallSSTBatchHeaderSize+(oldCount+newCount)*8 {
		return 0, 0, ErrCorruptWAL
	}
	offset := walInstallSSTBatchHeaderSize
	for i := 0; i < oldCount+newCount; i++ {
		fileNo := binary.LittleEndian.Uint64(payload[offset : offset+8])
		if !validRecordFileNo(fileNo) {
			return 0, 0, ErrCorruptWAL
		}
		offset += 8
	}
	return oldCount, newCount, nil
}

func validRecordFileNo(fileNo uint64) bool {
	return fileNo != 0 && fileNo < recordFileNoLimit
}

func parquetStoreFileNos(stores []*parquetRecordStore) []uint64 {
	fileNos := make([]uint64, len(stores))
	for i, store := range stores {
		fileNos[i] = store.fileNo
	}
	return fileNos
}

func recordFileNoSet(fileNos []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(fileNos))
	for _, fileNo := range fileNos {
		set[fileNo] = struct{}{}
	}
	return set
}

func cleanupMajorCompactionSSTs(stores []*parquetRecordStore) error {
	var firstErr error
	removed := false
	for _, store := range stores {
		if store == nil {
			continue
		}
		if err := store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.Remove(store.path); err == nil {
			removed = true
		} else if !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil || !removed || len(stores) == 0 {
		return firstErr
	}
	return syncDir(filepath.Dir(stores[0].path))
}
