package minweight_store

import "encoding/binary"

func isZeroBytes(data []byte) bool {
	fullWordBytes := len(data) &^ 7
	for i := 0; i < fullWordBytes; i += 8 {
		if binary.LittleEndian.Uint64(data[i:i+8]) != 0 {
			return false
		}
	}
	for _, b := range data[fullWordBytes:] {
		if b != 0 {
			return false
		}
	}
	return true
}
