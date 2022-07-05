package file

import (
	"encoding/binary"
	"errors"
	"github.com/AllenShaw19/paxos/paxos/utils/bin"
	"github.com/AllenShaw19/paxos/plugin/log"
	"io"
	"os"
)

type File struct {
	path string
	f    *os.File
}

func OpenFile(path string) (*File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Error("open file fail", log.String("path", path), log.Err(err))
		return nil, err
	}
	f := &File{path: path, f: file}
	return f, nil
}

func (f *File) SeekToStart() error {
	ret, err := f.f.Seek(0, io.SeekStart)
	if err != nil {
		log.Error("file seed to start fail", log.String("path", f.path), log.Err(err))
		return err
	}
	if ret != 0 {
		log.Error("file seed to start fail, not ret 0", log.Int64("ret", ret))
		return errors.New("seed to start fail, invalid ret")
	}
	return nil
}

func (f *File) ReadUint64() (uint64, error) {
	buf := make([]byte, 0, bin.SizeOfUint64)
	n, err := f.f.Read(buf)
	if err != nil {
		log.Error("read uint64 fail", log.Err(err))
		return 0, err
	}

	if n != bin.SizeOfUint64 {
		log.Error("read uint64 fail, read len not size of uint64", log.Int("read_len", n))
		return 0, errors.New("read uint64 fail, read len invalid")
	}

	value := binary.BigEndian.Uint64(buf)
	return value, nil
}

func (f *File) ReadUint32() (uint32, error) {
	buf := make([]byte, 0, bin.SizeOfUint32)
	n, err := f.f.Read(buf)
	if err != nil {
		log.Error("read uint32 fail", log.Err(err))
		return 0, err
	}
	if n != bin.SizeOfUint32 {
		log.Error("read uint32 fail, read len not size of uint32", log.Int("read_len", n))
		return 0, errors.New("read uint32 fail, read len invalid")
	}
	value := binary.BigEndian.Uint32(buf)
	return value, nil
}
