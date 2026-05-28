package minweight_store

import "testing"

const bitsetBenchBits = 4094

func BenchmarkBitsetFirstZero(b *testing.B) {
	cases := []struct {
		name     string
		freeSlot uint64
		full     bool
	}{
		{name: "first", freeSlot: 0},
		{name: "middle", freeSlot: bitsetBenchBits / 2},
		{name: "last", freeSlot: bitsetBenchBits - 1},
		{name: "full", full: true},
	}

	for _, tc := range cases {
		b.Run(tc.name+"/word", func(b *testing.B) {
			bitmap := fullBitsetBitmapForBench()
			if !tc.full {
				bitsetSet(bitmap, tc.freeSlot, false)
			}
			b.ReportAllocs()
			b.ResetTimer()

			var slot uint64
			var ok bool
			for i := 0; i < b.N; i++ {
				slot, ok = bitsetFirstZero(bitmap, bitsetBenchBits)
			}
			if ok == tc.full {
				b.Fatalf("ok = %v, full = %v", ok, tc.full)
			}
			if ok && slot != tc.freeSlot {
				b.Fatalf("slot = %d, want %d", slot, tc.freeSlot)
			}
		})
		b.Run(tc.name+"/byte", func(b *testing.B) {
			bitmap := fullBitsetBitmapForBench()
			if !tc.full {
				bitsetSet(bitmap, tc.freeSlot, false)
			}
			b.ReportAllocs()
			b.ResetTimer()

			var slot uint64
			var ok bool
			for i := 0; i < b.N; i++ {
				slot, ok = bitsetFirstZeroByteScanForBench(bitmap, bitsetBenchBits)
			}
			if ok == tc.full {
				b.Fatalf("ok = %v, full = %v", ok, tc.full)
			}
			if ok && slot != tc.freeSlot {
				b.Fatalf("slot = %d, want %d", slot, tc.freeSlot)
			}
		})
	}
}

func BenchmarkBitsetCount(b *testing.B) {
	bitmap := fullBitsetBitmapForBench()
	for slot := uint64(0); slot < bitsetBenchBits; slot += 3 {
		bitsetSet(bitmap, slot, false)
	}
	b.ReportAllocs()
	b.ResetTimer()

	var count uint64
	for i := 0; i < b.N; i++ {
		count = bitsetCount(bitmap, bitsetBenchBits)
	}
	if count == 0 {
		b.Fatal("count = 0")
	}
}

func fullBitsetBitmapForBench() []byte {
	bitmap := make([]byte, (bitsetBenchBits+7)/8)
	for byteIndex := range bitmap {
		bitmap[byteIndex] = bitsetByteMask(byteIndex, bitsetBenchBits)
	}
	return bitmap
}

func bitsetFirstZeroByteScanForBench(bitmap []byte, bitCount uint64) (uint64, bool) {
	byteCount := int((bitCount + 7) / 8)
	for byteIndex := 0; byteIndex < byteCount; byteIndex++ {
		usable := bitsetByteMask(byteIndex, bitCount)
		if bitmap[byteIndex]&usable == usable {
			continue
		}
		for bit := uint64(0); bit < 8; bit++ {
			mask := byte(1 << bit)
			if usable&mask == 0 || bitmap[byteIndex]&mask != 0 {
				continue
			}
			return uint64(byteIndex)*8 + bit, true
		}
	}
	return 0, false
}
