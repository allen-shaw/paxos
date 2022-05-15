package paxos

import "unsafe"

var (
	sizeOfInt    = int(unsafe.Sizeof(int(0)))
	sizeOfUint32 = int(unsafe.Sizeof(uint32(0)))
	sizeOfUint64 = int(unsafe.Sizeof(uint64(0)))
)
