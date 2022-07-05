package logstorage

import (
	"errors"
	"fmt"
	"github.com/AllenShaw19/paxos/paxos/utils/bin"
	"github.com/AllenShaw19/paxos/paxos/utils/crypto"
	"github.com/AllenShaw19/paxos/paxos/utils/file"
	"github.com/AllenShaw19/paxos/plugin/log"
	"io"
	"os"
	"path/filepath"
)

type LogStore struct {
	metaFile *file.File
	fileId   uint64

	groupIdx int
	path     string
}

func NewLogStore() *LogStore {

}

func (s *LogStore) Init(path string, groupIdx int, database *Database) error {
	s.groupIdx = groupIdx

	s.path = filepath.Join(path, "vfile")
	if !file.IsDir(s.path) {
		err := os.Mkdir(s.path, os.ModePerm)
		if err != nil {
			log.Error("create dir fail", log.String("path", s.path), log.Err(err))
			return err
		}
	}

	metaFilePath := filepath.Join(s.path, "meta")
	metaFile, err := file.OpenFile(metaFilePath)
	if err != nil {
		log.Error("open meta file fail", log.String("path", metaFilePath), log.Err(err))
		return err
	}
	s.metaFile = metaFile

	err = metaFile.SeekToStart()
	if err != nil {
		log.Error("file seek to start fail", log.Err(err))
		return err
	}

	s.fileId, err = metaFile.ReadUint64()
	if err != nil && !errors.Is(err, io.EOF) {
		log.Error("read file_id fail", log.Err(err))
		return err
	}

	metaChecksum, err := metaFile.ReadUint32()
	if err != nil {
		log.Error("read meta_checksum fail", log.Err(err))
		return err
	}

	if checksum := crypto.Crc32(s.fileId); checksum != metaChecksum {
		log.Error("meta file checksum verify fail",
			log.Uint32("meta checksum", metaChecksum),
			log.Uint32("cal checksum", checksum),
			log.Uint64("file id", s.fileId))
		return errors.New("meta file checksum fail")
	}

}

func (s *LogStore) RebuildIndex(database *Database) (int64, error) {
	lastFileId, currInstanceId, err := database.GetMaxInstanceIdAndFileId()
	if err != nil {
		log.Error("database get max_instance_id file_id fail", log.Err(err))
		return 0, nil
	}

	var (
		fileId   uint64
		offset   uint64
		checksum uint32
	)

	if lastFileId != nil {
		fileId, offset, checksum, err = s.parseFileId(lastFileId)
		if err != nil {
			log.Error("parse file id fail", log.ByteString("last file_id", lastFileId), log.Err(err))
			return 0, err
		}
	}

	if fileId > s.fileId {
		log.Error("pebbleDB last file_id larger than meta now file_id, file error",
			log.Uint64("pebbleDB last file_id", fileId),
			log.Uint64("meta file_id", s.fileId))
		return 0, errors.New("invalid file id")
	}

	log.Info(fmt.Sprintf("START fileid %d offset %d checksum %d", fileId, offset, checksum))

	var fileWriteOffset uint64
	for currFileId := fileId; ; currFileId++ {
		fileWriteOffset, currInstanceId, err := s.RebuildIndexForOneFile()

	}

}

func (s *LogStore) parseFileId(rawFileId []byte) (uint64, uint64, uint32, error) {
	buf := bin.NewBuffer(rawFileId)
	fileId, err := buf.ReadUint64()
	if err != nil {
		return 0, 0, 0, err
	}
	offset, err := buf.ReadUint64()
	if err != nil {
		return 0, 0, 0, err
	}
	checksum, err := buf.ReadUint32()
	if err != nil {
		return 0, 0, 0, err
	}
	return fileId, offset, checksum, nil
}

func (s *LogStore) RebuildIndexForOneFile(fileId, offset, currInstanceId uint64, database *Database) (uint64, uint64, error) {

	filePath := filepath.Join(s.path, fmt.Sprintf("%d.f", fileId))
	if !file.IsExists(filePath) {
		log.Error("file not exist", log.String("filepath", filePath))
		return 0, currInstanceId, ErrNotExist
	}
}
