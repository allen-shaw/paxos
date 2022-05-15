package paxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type LogStore struct {
	file            *os.File
	metaFile        *os.File
	fileID          int
	path            string
	tmpBuffer       []byte
	tmpAppendBuffer []byte

	mutex     sync.Mutex
	readMutex sync.RWMutex

	deletedMaxFileID int
	myGroupIdx       int
	nowFileSize      int
	nowFileOffset    int64
}

func NewLogStore() *LogStore {
	s := &LogStore{}
	s.file = nil
	s.metaFile = nil
	s.fileID = -1
	s.deletedMaxFileID = -1
	s.myGroupIdx = -1
	s.nowFileSize = -1
	s.nowFileOffset = 0
	return s
}

func (s *LogStore) Close() {
	if s.file != nil {
		s.file.Close()
	}
	if s.metaFile != nil {
		s.metaFile.Close()
	}
}

func (s *LogStore) Init(path string, myGroupIdx int, database *Database) error {
	s.myGroupIdx = myGroupIdx
	s.path = filepath.Join(path, "vfile")
	if !IsDir(s.path) {
		err := os.Mkdir(s.path, os.ModePerm)
		if err != nil {
			log.Error("create dir fail", log.String("path", s.path), log.Err(err))
			return err
		}
	}

	metaFilePath := filepath.Join(s.path, "meta")
	metaFile, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Error("open meta file fail", log.String("path", metaFilePath), log.Err(err))
		return err
	}
	s.metaFile = metaFile

	_, err = metaFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Error("seek meta file to 0 fail", log.String("path", metaFilePath), log.Err(err))
		return err
	}

	fileIDBuf := make([]byte, 0, sizeOfInt)
	readLen, err := metaFile.Read(fileIDBuf)
	if err != nil {
		log.Error("read meta file fail", log.String("path", metaFilePath), log.Err(err))
		return err
	}
	if readLen != sizeOfInt {
		if readLen == 0 {
			s.fileID = 0
		} else {
			log.Error("read meta info fail", log.Int("readlen", readLen))
			return errors.New("read len not expected")
		}
	}
	// fileID must larger than 0
	s.fileID = int(binary.BigEndian.Uint64(fileIDBuf))

	metaCheckSumBuf := make([]byte, 0, sizeOfUint32)
	readLen, err = metaFile.Read(metaCheckSumBuf)
	if err != nil {
		log.Error("read meta file checksum fail", log.Err(err))
		return err
	}

	var metaCheckSum uint32 = 0
	if readLen == sizeOfUint32 {
		metaCheckSum = binary.BigEndian.Uint32(metaCheckSumBuf)
		checkSum := crc32.ChecksumIEEE(fileIDBuf)
		if checkSum != metaCheckSum {
			log.Error("meta file checksum not same to cal checksum",
				log.Uint32("meta checksum", metaCheckSum),
				log.Uint32("cal checksum", checkSum),
				log.Int("file id", s.fileID))
			return errors.New("meta file checksum fail")
		}
	}

	s.nowFileOffset, err = s.RebuildIndex(database)
	if err != nil {
		log.Error("rebuild index fail", log.Err(err))
		return err
	}

	s.file, err = s.openFile(s.fileID)
	if err != nil {
		log.Error("open file fail", log.Err(err))
		return err
	}

	s.nowFileSize, err = s.expandFile(s.file)
	if err != nil {
		log.Error("expand file fail", log.Err(err))
		return err
	}

	s.nowFileOffset, err = s.file.Seek(s.nowFileOffset, io.SeekStart)
	if err != nil {
		log.Error("seek to now file offset fail", log.Int64("now file offset", s.nowFileOffset))
		return err
	}

	log.Info("log store init success.", log.String("path", path),
		log.Int("file_id", s.fileID),
		log.Uint32("meta_checksum", metaCheckSum),
		log.Int("now_file_size", s.nowFileSize),
		log.Int64("now_file_write_offset", s.nowFileOffset))

	return nil
}

func (s *LogStore) RebuildIndex(database *Database) (int64, error) {
	lastFileID, nowInstanceID, err := database.GetMaxInstanceIDFileID()
	if err != nil {
		log.Error("database get max_instance_id file_id fail", log.Err(err))
		return 0, nil
	}

	var (
		fileID   int
		offset   int64
		checkSum uint32
	)

	if len(lastFileID) > 0 {
		fileID, offset, checkSum, err = s.parseFileID(lastFileID)
		if err != nil {
			log.Error("parse file id fail", log.String("last file_id", lastFileID), log.Err(err))
			return 0, err
		}
	}

	if fileID > s.fileID {
		log.Error("db last file_id larger than meta now file_id, file error",
			log.Int("db last file_id", fileID),
			log.Int("meta file_id", s.fileID))
		return 0, errors.New("invalid file id")
	}

	log.Info(fmt.Sprintf("START fileid %d offset %d checksum %d", fileID, offset, checkSum))

	var nowFileWriteOffset int64
	for nowFileID := fileID; ; nowFileID++ {
		nowFileWriteOffset, nowInstanceID, err = s.RebuildIndexForOneFile(nowFileID, offset, database, nowInstanceID)
		if err != nil && err != ErrNotExist {
			log.Error("rebuild index for one file fail.", log.Err(err))
			break
		} else if err == ErrNotExist {
			if nowFileID != 0 && nowFileID != s.fileID+1 {
				log.Error("meta file wrong.", log.Int("now file_id", nowFileID), log.Int("meta.now file_id", s.fileID))
				return nowFileWriteOffset, errors.New("")
			}
			log.Info("end rebuild ok.", log.Int("now file_id", nowFileID))
			break
		}

		offset = 0
	}

	return nowFileWriteOffset, err
}

func (s *LogStore) RebuildIndexForOneFile(fileID int, offset int64, database *Database, nowInstanceID uint64) (int64, uint64, error) {
	var (
		nowFileWriteOffset int64
		err                error
	)

	filePath := fmt.Sprintf("%s/%d.f", s.path, fileID)

	if !Exists(filePath) {
		log.Error("file not exist", log.String("filepath", filePath))
		return 0, nowInstanceID, ErrNotExist
	}

	f, err := s.openFile(fileID)
	if err != nil {
		return 0, nowInstanceID, err
	}
	defer f.Close()

	fileLen, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nowInstanceID, err
	}
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return 0, nowInstanceID, err
	}

	nowOffset := offset
	needTruncate := false

	for {
		var readLen int
		lenBuf := make([]byte, 0, sizeOfInt)
		readLen, err = f.Read(lenBuf)
		if err != nil && err != io.EOF {
			log.Error("read file fail", log.Err(err))
			break
		}
		if err == io.EOF && readLen == 0 {
			log.Error("file end", log.Int("file_id", fileID), log.Int64("offset", nowOffset))
			nowFileWriteOffset = nowOffset
			break
		}

		if readLen != sizeOfInt {
			needTruncate = true
			log.Error("readlen not equal to int size, need truncate", log.Int("read_len", readLen), log.Int("size of int", sizeOfInt))
			break
		}

		dataLen := int(binary.BigEndian.Uint64(lenBuf))
		if dataLen == 0 {
			log.Info("file data end", log.Int("file_id", fileID), log.Int64("offset", nowOffset))
			nowFileWriteOffset = nowOffset
			break
		}

		if dataLen > int(fileLen) || dataLen < sizeOfUint64 {
			log.Error("file data len wrong", log.Int("data len", dataLen), log.Int64("file len", fileLen))
			err = errors.New("invalid file data len")
			break
		}

		s.tmpBuffer = make([]byte, 0, dataLen)
		readLen, err = f.Read(s.tmpBuffer)
		if err != nil {
			log.Error("read file data fail", log.Err(err))
			break
		}
		if readLen != dataLen {
			needTruncate = true
			log.Error("readlen not equal to datalen", log.Int("readlen", readLen), log.Int("datalen", dataLen))
			break
		}

		instanceID := binary.BigEndian.Uint64(s.tmpBuffer[0:sizeOfUint64])
		if instanceID < nowInstanceID {
			log.Error("file data wrong, read instanceid smaller than now instanceid",
				log.Uint64("read_instance_id", instanceID),
				log.Uint64("now_instance_id", nowInstanceID))
			err = errors.New("error instanceid in file data")
			break
		}
		nowInstanceID = instanceID

		state := &AcceptorStateData{}
		err = proto.Unmarshal(s.tmpBuffer[sizeOfUint64:], state)
		if err != nil {
			s.nowFileOffset = nowOffset
			log.Error("This instance's buffer wrong, can't parse to acceptState.",
				log.Uint64("instance_id", instanceID),
				log.Int("buffer_len", dataLen-sizeOfUint64),
				log.Int64("now_offset", nowOffset))
			needTruncate = true
			break
		}

		fileCheckSum := crc32.ChecksumIEEE(s.tmpBuffer)
		fID := s.genFileID(fileID, nowOffset, fileCheckSum)

		err = database.ReBuildOneIndex(instanceID, fID)
		if err != nil {
			break
		}

		log.Info("rebuild one index ok.", log.Int("fileid", fileID),
			log.Int64("offset", nowOffset),
			log.Uint64("instance_id", instanceID),
			log.Uint32("checksum", fileCheckSum),
			log.Int("buffer_size", dataLen-sizeOfUint64))
		nowOffset += int64(sizeOfInt + dataLen)
	}

	if needTruncate {
		log.Info("truncate file", log.Int("file_id", fileID), log.Int64("offset", offset), log.Int64("filesize", fileLen))
		err := f.Truncate(int64(nowOffset))
		if err != nil {
			log.Error("truncate fail.", log.String("file_path", filePath), log.Int64("truncate to length", nowOffset), log.Err(err))
			return 0, nowInstanceID, err
		}
	}

	return nowFileWriteOffset, nowInstanceID, err
}

func (s *LogStore) parseFileID(fileID string) (int, int64, uint32, error) {
	buf := bytes.NewBufferString(fileID)
	fileIDBuf := make([]byte, 0, sizeOfInt)
	offsetBuf := make([]byte, 0, sizeOfInt)
	checkSumBuf := make([]byte, 0, sizeOfUint32)
	_, err := buf.Read(fileIDBuf)
	if err != nil {
		return 0, 0, 0, err
	}
	_, err = buf.Read(offsetBuf)
	if err != nil {
		return 0, 0, 0, err
	}
	_, err = buf.Read(checkSumBuf)
	if err != nil {
		return 0, 0, 0, err
	}
	fID := int(binary.BigEndian.Uint64(fileIDBuf))
	offset := int64(binary.BigEndian.Uint64(offsetBuf))
	checkSum := binary.BigEndian.Uint32(checkSumBuf)
	return fID, offset, checkSum, nil
}

func (s *LogStore) genFileID(fileID int, offset int64, checkSum uint32) string {
	tmp := make([]byte, 0, sizeOfInt+sizeOfInt+sizeOfUint32)

	fileIDBuf := make([]byte, 0, sizeOfInt)
	binary.BigEndian.PutUint64(fileIDBuf, uint64(fileID))

	offsetBuf := make([]byte, 0, sizeOfInt)
	binary.BigEndian.PutUint64(offsetBuf, uint64(offset))

	checkSumBuf := make([]byte, 0, sizeOfUint32)
	binary.BigEndian.PutUint32(checkSumBuf, checkSum)

	tmp = append(tmp, fileIDBuf...)
	tmp = append(tmp, offsetBuf...)
	tmp = append(tmp, checkSumBuf...)

	return string(tmp)
}

func (s *LogStore) openFile(fileID int) (*os.File, error) {
	filePath := fmt.Sprintf("%s/%d.f", s.path, fileID)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Error("open file fail", log.String("filepath", filePath), log.Err(err))
		return nil, err
	}
	log.Info("ok, open file success", log.String("path", filePath))
	return f, nil
}

func (s *LogStore) expandFile(file *os.File) (int, error) {
	fileSize, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if fileSize == 0 {
		fileSize, err = file.Seek(int64(LogFileMaxSize()), io.SeekStart)
		if err != nil || fileSize != int64(LogFileMaxSize())-1 {
			log.Error("seek fail", log.Err(err), log.Int64("seek_ret", fileSize))
			return 0, fmt.Errorf("seek fail with %v, %d", err, fileSize)
		}

		writeLen, err := file.Write([]byte{0})
		if err != nil || writeLen != 1 {
			log.Error("write 1 bytes fail", log.Err(err), log.Int("write_len", writeLen))
			return 0, fmt.Errorf("write eof fail with %v, %d", err, writeLen)
		}

		fileSize = int64(LogFileMaxSize())
		offset, err := file.Seek(0, io.SeekStart)
		s.nowFileOffset = 0
		if err != nil || offset != 0 {
			log.Error("file seed to 0 fail", log.Int64("offset", offset), log.Err(err))
			return 0, fmt.Errorf("seek to start fail with %v, %d", err, offset)
		}
	}

	return int(fileSize), nil
}
