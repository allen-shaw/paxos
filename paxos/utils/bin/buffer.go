package bin

import (
	"bytes"
	"encoding/binary"
)

type Buffer struct {
	buff *bytes.Buffer
}

func NewBuffer(buf []byte) *Buffer {
	return &Buffer{
		buff: bytes.NewBuffer(buf),
	}
}

func (b *Buffer) ReadUint64() (uint64, error) {
	buff := make([]byte, 0, SizeOfUint64)
	_, err := b.buff.Read(buff)
	if err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint64(buff)
	return v, nil
}

func (b *Buffer) ReadUint32() (uint32, error) {
	buff := make([]byte, 0, SizeOfUint32)
	_, err := b.buff.Read(buff)
	if err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(buff)
	return v, nil
}
