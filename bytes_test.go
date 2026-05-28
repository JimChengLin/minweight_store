package minweight_store

import "testing"

func TestIsZeroBytes(t *testing.T) {
	for _, size := range []int{0, 1, 7, 8, 9, walHeaderSize} {
		data := make([]byte, size)
		if !isZeroBytes(data) {
			t.Fatalf("isZeroBytes(%d zeros) = false, want true", size)
		}
		if size == 0 {
			continue
		}
		data[size-1] = 1
		if isZeroBytes(data) {
			t.Fatalf("isZeroBytes(%d bytes with tail non-zero) = true, want false", size)
		}
	}
}
