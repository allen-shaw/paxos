package paxos

import (
	"errors"
	"fmt"
	"github.com/AllenShaw19/paxos/plugin/log"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type CheckpointReceiver struct {
	config     *Config
	logStorage LogStorage

	senderNodeID NodeID
	uuid         uint64
	sequence     uint64

	hasInitDir map[string]bool
}

func NewCheckpointReceiver(config *Config, logStorage LogStorage) *CheckpointReceiver {
	r := &CheckpointReceiver{config: config, logStorage: logStorage}
	r.Reset()
	return r
}

func (r *CheckpointReceiver) Reset() {
	r.hasInitDir = nil
	r.senderNodeID = nilNode
	r.uuid = 0
	r.sequence = 0
}

func (r *CheckpointReceiver) NewReceiver(senderNodeID NodeID, uuid uint64) error {
	err := r.clearCheckpointTmp()
	if err != nil {
		return err
	}

	groupIdx := r.config.GetMyGroupIdx()

	err = r.logStorage.ClearAllLog(groupIdx)
	if err != nil {
		log.Error("clear all log fail", log.Int("group_idx", groupIdx), log.Err(err))
		return err
	}

	r.hasInitDir = make(map[string]bool)
	r.senderNodeID = senderNodeID
	r.uuid = uuid
	r.sequence = 0

	return nil
}

func (r *CheckpointReceiver) IsReceiverFinish(senderNodeID NodeID, uuid uint64, endSequence uint64) bool {
	if senderNodeID == r.senderNodeID &&
		uuid == r.uuid && endSequence == r.sequence+1 {
		return true
	}

	return false
}

func (r *CheckpointReceiver) GetTmpDirPath(smID int) string {
	groupIdx := r.config.GetMyGroupIdx()
	logStoragePath := r.logStorage.GetLogStorageDirPath(groupIdx)

	tmpDirPath := fmt.Sprintf("%s/cp_tmp_%d", logStoragePath, smID)
	return tmpDirPath
}

func (r *CheckpointReceiver) ReceiveCheckpoint(msg *CheckpointMsg) error {
	if NodeID(msg.NodeID) != r.senderNodeID || msg.UUID != r.uuid {
		log.Error("msg invalid", log.Uint64("msg.sender_node_id", msg.NodeID),
			log.Uint64("receiver,sender_node_id", uint64(r.senderNodeID)),
			log.Uint64("msg.uuid", msg.UUID),
			log.Uint64("receiver.uuid", r.uuid))
		return ErrInvalidMsg
	}

	if msg.Sequence == r.sequence {
		log.Error("msg already receive, skip", log.Uint64("msg.sequence", msg.Sequence),
			log.Uint64("receiver.sequence", r.sequence))
		return nil
	}

	if msg.Sequence != r.sequence+1 {
		log.Error("msg sequence invalid", log.Uint64("msg.sequence", msg.Sequence),
			log.Uint64("receiver.sequence", r.sequence))
		return ErrInvalidMsg
	}

	filePath := r.GetTmpDirPath(int(msg.SMID))
	formatFilePath, err := r.InitFilePath(filePath)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(formatFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		log.Error("open file fail", log.String("filepath", filePath), log.Err(err))
		return err
	}
	defer file.Close()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Error("file.seek fail", log.Err(err))
		return err
	}
	if uint64(offset) != msg.Offset {
		log.Error("file msg err", log.Int64("file.offset", offset),
			log.Uint64("msg.offset", msg.Offset))
		return errors.New("file msg error")
	}

	writeLen, err := file.Write(msg.Buffer)
	if err != nil || writeLen != len(msg.Buffer) {
		log.Error("file write fail", log.Err(err), log.Int("write_len", writeLen),
			log.Int("buffer_size", len(msg.Buffer)))
		return err
	}

	r.sequence++

	log.Info("ReceiveCheckpoint END", log.Int("write_len", writeLen))
	return nil
}

func (r *CheckpointReceiver) InitFilePath(filePath string) (string, error) {
	log.Info("START Init file path", log.String("filepath", filePath))

	err := r.CreateDir(filePath)
	if err != nil {
		return "", err
	}

	r.hasInitDir[filePath] = true
	return filePath, nil
}

func (r *CheckpointReceiver) clearCheckpointTmp() error {
	groupIdx := r.config.GetMyGroupIdx()
	logStoragePath := r.logStorage.GetLogStorageDirPath(groupIdx)

	dir, err := os.ReadDir(logStoragePath)
	if err != nil {
		return err
	}

	for _, entry := range dir {
		if strings.Contains(entry.Name(), "cp_tmp_") {
			childPath := filepath.Join(logStoragePath, entry.Name())
			err := os.RemoveAll(childPath)
			if err != nil {
				log.Error("rm dir fail", log.String("dir", childPath), log.Err(err))
				return err
			}
			log.Error("rm dir done", log.String("dir", childPath))
		}
	}

	return nil
}

func (r *CheckpointReceiver) CreateDir(dirPath string) error {
	if !IsExists(dirPath) {
		err := os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			log.Error("create dir fail", log.String("path", dirPath))
			return err
		}
	}
	return nil
}
