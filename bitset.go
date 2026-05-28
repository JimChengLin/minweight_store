package minweight_store

import (
	"encoding/binary"
	"math/bits"
)

func bitsetFirstZero(bitmap []byte, bitCount uint64) (uint64, bool) {
	byteCount := int((bitCount + 7) / 8)
	fullWordBytes := byteCount &^ 7
	for byteIndex := 0; byteIndex < fullWordBytes; byteIndex += 8 {
		free := ^binary.LittleEndian.Uint64(bitmap[byteIndex:byteIndex+8]) & bitsetWordMask(byteIndex, bitCount)
		if free == 0 {
			continue
		}
		return uint64(byteIndex)*8 + uint64(bits.TrailingZeros64(free)), true
	}
	for byteIndex := fullWordBytes; byteIndex < byteCount; byteIndex++ {
		usable := bitsetByteMask(byteIndex, bitCount)
		if bitmap[byteIndex]&usable == usable {
			continue
		}
		free := ^bitmap[byteIndex] & usable
		return uint64(byteIndex)*8 + uint64(bits.TrailingZeros8(free)), true
	}
	return 0, false
}

func bitsetGet(bitmap []byte, bit uint64) bool {
	mask := byte(1 << (bit % 8))
	return bitmap[bit/8]&mask != 0
}

func bitsetSet(bitmap []byte, bit uint64, value bool) {
	mask := byte(1 << (bit % 8))
	if value {
		bitmap[bit/8] |= mask
		return
	}
	bitmap[bit/8] &^= mask
}

func bitsetCount(bitmap []byte, bitCount uint64) uint64 {
	var count uint64
	byteCount := int((bitCount + 7) / 8)
	fullWordBytes := byteCount &^ 7
	for byteIndex := 0; byteIndex < fullWordBytes; byteIndex += 8 {
		word := binary.LittleEndian.Uint64(bitmap[byteIndex:byteIndex+8]) & bitsetWordMask(byteIndex, bitCount)
		count += uint64(bits.OnesCount64(word))
	}
	for byteIndex := fullWordBytes; byteIndex < byteCount; byteIndex++ {
		count += uint64(bits.OnesCount8(bitmap[byteIndex] & bitsetByteMask(byteIndex, bitCount)))
	}
	return count
}

func bitsetWordMask(byteIndex int, bitCount uint64) uint64 {
	remaining := bitCount - uint64(byteIndex)*8
	if remaining >= 64 {
		return ^uint64(0)
	}
	return 1<<remaining - 1
}

func bitsetByteMask(byteIndex int, bitCount uint64) byte {
	remaining := bitCount - uint64(byteIndex)*8
	if remaining >= 8 {
		return 0xff
	}
	return byte(1<<remaining - 1)
}
