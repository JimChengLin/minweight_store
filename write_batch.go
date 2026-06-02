package minweight_store

import (
	"encoding/binary"

	"github.com/JimChengLin/minpatricia"
)

type WriteBatch struct {
	ops []writeBatchOperation
}

type writeBatchOperation struct {
	op    byte
	key   []byte
	value []byte
}

type writeBatchRecord struct {
	op  byte
	key []byte
	pos minpatricia.Position
}

func (b *WriteBatch) Put(key, value []byte) error {
	if len(key) > minpatricia.MaxKeySize {
		return minpatricia.ErrKeyTooLarge
	}
	b.ops = append(b.ops, writeBatchOperation{
		op:    walOpPut,
		key:   cloneBytes(key),
		value: cloneBytes(value),
	})
	return nil
}

func (b *WriteBatch) Delete(key []byte) error {
	if len(key) > minpatricia.MaxKeySize {
		return minpatricia.ErrKeyTooLarge
	}
	b.ops = append(b.ops, writeBatchOperation{
		op:  walOpDelete,
		key: cloneBytes(key),
	})
	return nil
}

func (b *WriteBatch) Len() int {
	return len(b.ops)
}

func (b *WriteBatch) Reset() {
	for i := range b.ops {
		b.ops[i] = writeBatchOperation{}
	}
	b.ops = b.ops[:0]
}

func encodeWriteBatchPayload(ops []writeBatchOperation, maxPayload uint64) ([]byte, []uint64, error) {
	if len(ops) == 0 {
		return nil, nil, ErrCorruptWAL
	}

	total := uint64(0)
	for _, op := range ops {
		if op.op != walOpPut && op.op != walOpDelete {
			return nil, nil, ErrCorruptWAL
		}
		if op.op == walOpDelete && len(op.value) != 0 {
			return nil, nil, ErrCorruptWAL
		}
		total += uint64(walRecordHeaderSize) + uint64(len(op.key)) + uint64(len(op.value))
		if total > maxPayload {
			return nil, nil, ErrWalFull
		}
	}

	payload := make([]byte, int(total))
	relativeOffsets := make([]uint64, len(ops))
	offset := 0
	for i, op := range ops {
		relativeOffsets[i] = uint64(offset)
		entryOp := byte(walBatchEntryPut)
		if op.op == walOpDelete {
			entryOp = walBatchEntryDelete
		}
		entry := payload[offset : offset+walRecordHeaderSize+len(op.key)+len(op.value)]
		entry[walRecordOpOffset] = entryOp
		binary.LittleEndian.PutUint32(entry[walRecordKeyOffset:walRecordKeyOffset+4], uint32(len(op.key)))
		binary.LittleEndian.PutUint32(entry[walRecordValueOffset:walRecordValueOffset+4], uint32(len(op.value)))
		copy(entry[walRecordHeaderSize:], op.key)
		copy(entry[walRecordHeaderSize+len(op.key):], op.value)
		offset += len(entry)
	}
	return payload, relativeOffsets, nil
}

func forEachWriteBatchPayloadEntry(payload []byte, fn func(op byte, key, value []byte, relativeOffset uint64) error) error {
	if len(payload) == 0 {
		return ErrCorruptWAL
	}
	offset := 0
	for offset < len(payload) {
		if offset+walRecordHeaderSize > len(payload) {
			return ErrCorruptWAL
		}
		header := payload[offset : offset+walRecordHeaderSize]
		var op byte
		switch header[walRecordOpOffset] {
		case walBatchEntryPut:
			op = walOpPut
		case walBatchEntryDelete:
			op = walOpDelete
		default:
			return ErrCorruptWAL
		}
		keyLen := int(binary.LittleEndian.Uint32(header[walRecordKeyOffset : walRecordKeyOffset+4]))
		valueLen := int(binary.LittleEndian.Uint32(header[walRecordValueOffset : walRecordValueOffset+4]))
		if keyLen > minpatricia.MaxKeySize {
			return ErrCorruptWAL
		}
		if op == walOpDelete && valueLen != 0 {
			return ErrCorruptWAL
		}
		end := offset + walRecordHeaderSize + keyLen + valueLen
		if end < offset || end > len(payload) {
			return ErrCorruptWAL
		}
		keyStart := offset + walRecordHeaderSize
		valueStart := keyStart + keyLen
		if fn != nil {
			if err := fn(op, payload[keyStart:valueStart], payload[valueStart:end], uint64(offset)); err != nil {
				return err
			}
		}
		offset = end
	}
	return nil
}
