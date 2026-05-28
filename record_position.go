package minweight_store

import "github.com/JimChengLin/minpatricia"

const (
	recordOffsetBits  = 30
	recordOffsetLimit = uint64(1) << recordOffsetBits
	recordOffsetMask  = recordOffsetLimit - 1
	recordFileNoLimit = uint64(1) << (63 - recordOffsetBits)
)

func makeRecordPosition(fileNo, offset uint64) (minpatricia.Position, error) {
	if fileNo == 0 || fileNo >= recordFileNoLimit || offset >= recordOffsetLimit {
		return 0, minpatricia.ErrPositionTag
	}
	pos := (fileNo << recordOffsetBits) | offset
	if pos&minpatriciaHandleTag != 0 {
		return 0, minpatricia.ErrPositionTag
	}
	return minpatricia.Position(pos), nil
}

func recordPositionFileNo(pos minpatricia.Position) uint64 {
	return uint64(pos) >> recordOffsetBits
}

func recordPositionOffset(pos minpatricia.Position) uint64 {
	return uint64(pos) & recordOffsetMask
}
