package crypto

import (
	"github.com/AllenShaw19/paxos/paxos/utils/bin"
	"hash/crc32"
)

func Crc32(data interface{}) uint32 {
	switch data.(type) {
	case string:
		return crc32.ChecksumIEEE([]byte(data.(string)))
	case []byte:
		return crc32.ChecksumIEEE(data.([]byte))
	case int, uint16, uint32, uint64:
		buff := bin.IntToBytes(data)
		return crc32.ChecksumIEEE(buff)
	default:
		return 0
	}
}
