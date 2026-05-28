package minweight_store

import "testing"

func BenchmarkIsZeroBytes(b *testing.B) {
	cases := []struct {
		name     string
		nonZero  int
		allZeros bool
	}{
		{name: "first", nonZero: 0},
		{name: "middle", nonZero: walHeaderSize / 2},
		{name: "last", nonZero: walHeaderSize - 1},
		{name: "zero", allZeros: true},
	}

	for _, tc := range cases {
		b.Run(tc.name+"/word", func(b *testing.B) {
			data := make([]byte, walHeaderSize)
			if !tc.allZeros {
				data[tc.nonZero] = 1
			}
			b.ReportAllocs()
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = isZeroBytes(data)
			}
			if ok != tc.allZeros {
				b.Fatalf("ok = %v, want %v", ok, tc.allZeros)
			}
		})
		b.Run(tc.name+"/byte", func(b *testing.B) {
			data := make([]byte, walHeaderSize)
			if !tc.allZeros {
				data[tc.nonZero] = 1
			}
			b.ReportAllocs()
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = isZeroBytesByteScanForBench(data)
			}
			if ok != tc.allZeros {
				b.Fatalf("ok = %v, want %v", ok, tc.allZeros)
			}
		})
	}
}

func isZeroBytesByteScanForBench(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
