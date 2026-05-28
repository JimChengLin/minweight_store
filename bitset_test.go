package minweight_store

import "testing"

func TestBitsetFirstZeroAndCount(t *testing.T) {
	const bitCount = 70
	bitmap := make([]byte, (bitCount+7)/8)
	for i := uint64(0); i < bitCount; i++ {
		bitsetSet(bitmap, i, true)
	}
	bitsetSet(bitmap, 63, false)

	slot, ok := bitsetFirstZero(bitmap, bitCount)
	if !ok || slot != 63 {
		t.Fatalf("first zero = (%d,%v), want 63,true", slot, ok)
	}
	if got := bitsetCount(bitmap, bitCount); got != bitCount-1 {
		t.Fatalf("count = %d, want %d", got, bitCount-1)
	}

	bitsetSet(bitmap, 63, true)
	slot, ok = bitsetFirstZero(bitmap, bitCount)
	if ok || slot != 0 {
		t.Fatalf("full first zero = (%d,%v), want 0,false", slot, ok)
	}
	if got := bitsetCount(bitmap, bitCount); got != bitCount {
		t.Fatalf("full count = %d, want %d", got, bitCount)
	}
}
