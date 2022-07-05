package bin

import (
	"encoding/binary"
	"unsafe"
)

var (
	SizeOfInt    = int(unsafe.Sizeof(int(0)))
	SizeOfUint16 = int(unsafe.Sizeof(uint16(0)))
	SizeOfUint32 = int(unsafe.Sizeof(uint32(0)))
	SizeOfUint64 = int(unsafe.Sizeof(uint64(0)))
)

func IntToBytes(i interface{}) []byte {
	switch i.(type) {
	case int:
		buff := make([]byte, 0, SizeOfInt)
		binary.BigEndian.PutUint64(buff, uint64(i.(int)))
		return buff
	case uint16:
		buff := make([]byte, 0, SizeOfUint16)
		binary.BigEndian.PutUint16(buff, i.(uint16))
		return buff
	case uint32:
		buff := make([]byte, 0, SizeOfUint32)
		binary.BigEndian.PutUint32(buff, i.(uint32))
		return buff
	case uint64:
		buff := make([]byte, 0, SizeOfUint64)
		binary.BigEndian.PutUint64(buff, i.(uint64))
		return buff
	default:
		return nil
	}
}
