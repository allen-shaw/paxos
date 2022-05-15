package paxos

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func IsExists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func Crc32(data interface{}) uint32 {
	switch data.(type) {
	case []byte:
		return crc32.ChecksumIEEE(data.([]byte))
	case int:
		buff := IntToBytes(data.(int))
		return crc32.ChecksumIEEE(buff)
	default:
		return 0
	}
}

func IntToBytes(i interface{}) []byte {
	switch i.(type) {
	case int:
		buff := make([]byte, 0, sizeOfInt)
		binary.BigEndian.PutUint64(buff, uint64(i.(int)))
		return buff
	case uint32:
		buff := make([]byte, 0, sizeOfUint32)
		binary.BigEndian.PutUint32(buff, i.(uint32))
		return buff
	case uint64:
		buff := make([]byte, 0, sizeOfUint64)
		binary.BigEndian.PutUint64(buff, i.(uint64))
		return buff
	default:
		return nil
	}
}
