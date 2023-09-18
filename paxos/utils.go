package paxos

import (
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
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
	case string:
		return crc32.ChecksumIEEE([]byte(data.(string)))
	case []byte:
		return crc32.ChecksumIEEE(data.([]byte))
	case int, uint16, uint32, uint64:
		buff := IntToBytes(data)
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
	case uint16:
		buff := make([]byte, 0, sizeOfUint16)
		binary.BigEndian.PutUint16(buff, i.(uint16))
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

func GetCurrentTimeMs() uint64 {
	return uint64(time.Now().UnixNano() / 1e6)
}

func GenGid(nodeID NodeID) uint64 {
	return uint64(nodeID) ^ uint64(rand.Uint32()) + uint64(rand.Uint32())
}

var goid uint64

func NewGoID() uint64 {
	return atomic.AddUint64(&goid, 1)
}

type TimeStat struct {
	timeMs uint64
}

func NewTimeStat() *TimeStat {
	return &TimeStat{
		timeMs: GetCurrentTimeMs(),
	}
}

func (t *TimeStat) Point() int {
	nowTime := GetCurrentTimeMs()

	passTime := 0
	if nowTime > t.timeMs {
		passTime = int(nowTime - t.timeMs)
	}
	t.timeMs = nowTime
	return passTime
}
