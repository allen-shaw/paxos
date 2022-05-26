package paxos

import (
	"fmt"
	"github.com/AllenShaw19/paxos/log"
	"github.com/go-errors/errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

const (
	CheckpointAckTimeout = 120000
	CheckpointAckLead    = 10
)

type CheckpointSender struct {
	sendNodeID NodeID

	config        *Config
	learner       *Learner
	smFac         *SMFac
	checkpointMgr *CheckpointMgr

	isEnd     bool
	isEnded   bool
	isStarted bool

	uuid     uint64
	sequence uint64

	ackSequence    uint64
	absLastAckTime uint64

	tmpBuffer       []byte
	alreadySentFile map[string]bool
}

func NewCheckpointSender(sendNodeID NodeID, config *Config,
	learner *Learner, smFac *SMFac, checkpointMgr *CheckpointMgr) *CheckpointSender {
	s := &CheckpointSender{}

	s.sendNodeID = sendNodeID
	s.config = config
	s.learner = learner
	s.smFac = smFac
	s.checkpointMgr = checkpointMgr

	s.isEnd = false
	s.isEnded = false
	s.isStarted = false
	s.uuid = (uint64(s.config.GetMyNodeID()) ^ learner.GetInstanceID()) + rand.Uint64()
	s.sequence = 0

	s.ackSequence = 0
	s.absLastAckTime = 0

	return s
}

func (s *CheckpointSender) Stop() {
	if s.isStarted && !s.isEnded {
		s.isEnd = true
	}
}

func (s *CheckpointSender) Run() {
	s.isStarted = true
	s.absLastAckTime = GetCurrentTimeMs()

	//pause checkpoint replayer
	needContinue := false
	for !s.checkpointMgr.GetReplayer().IsPaused() {
		if s.isEnd {
			s.isEnded = true
			return
		}

		needContinue = true
		s.checkpointMgr.GetReplayer().Pause()
		log.Info("wait replayer paused.")
		time.Sleep(100 * time.Millisecond)
	}

	err := s.lockCheckpoint()
	if err == nil {
		s.sendCheckpoint()
		s.unlockCheckpoint()
	}

	if needContinue {
		s.checkpointMgr.GetReplayer().Continue()
	}
	log.Info("checkpoint.sender [END]")
	s.isEnded = true
}

func (s *CheckpointSender) End() {
	s.isEnd = true
}

func (s *CheckpointSender) IsEnd() bool {
	return s.isEnded
}

func (s *CheckpointSender) Ack(sendNodeID NodeID, uuid, sequence uint64) {
	if sendNodeID != s.sendNodeID {
		log.Error("send nodeid not same",
			log.Uint64("ack.send_node_id", uint64(sendNodeID)),
			log.Uint64("self.send_node_id", uint64(s.sendNodeID)))
		return
	}

	if uuid != s.uuid {
		log.Error("uuid not same",
			log.Uint64("ack.uuid", uuid),
			log.Uint64("self.uuid", s.uuid))
		return
	}

	if sequence != s.ackSequence {
		log.Error("ack_sequence not same",
			log.Uint64("ack.sequence", sequence),
			log.Uint64("self.sequence", s.sequence))
	}

	s.ackSequence++
	s.absLastAckTime = GetCurrentTimeMs()
}

func (s *CheckpointSender) sendCheckpoint() error {
	groupIdx := s.config.GetMyGroupIdx()
	var err error

	for i := 0; i < 2; i++ {
		err = s.learner.SendCheckpointBegin(s.sendNodeID, s.uuid, s.sequence, s.smFac.GetCheckpointInstanceID(groupIdx))
		if err != nil {
			log.Error("send checkpoint begin fail", log.Err(err))
			return err
		}
	}

	s.sequence++

	smList := s.smFac.GetSMList()
	for _, sm := range smList {
		err = s.sendCheckpointFromSM(sm)
		if err != nil {
			return err
		}
	}

	err = s.learner.SendCheckpointEnd(s.sendNodeID, s.uuid, s.sequence, s.smFac.GetCheckpointInstanceID(groupIdx))
	if err != nil {
		log.Error("send checkpoint end fail", log.Uint64("sequence", s.sequence), log.Err(err))
		return err
	}

	return err
}

func (s *CheckpointSender) lockCheckpoint() error {
	smList := s.smFac.GetSMList()
	lockedSMList := make([]StateMachine, 0, len(smList))

	var err error
	for _, sm := range smList {
		err = sm.LockCheckpointState()
		if err != nil {
			break
		}

		lockedSMList = append(lockedSMList, sm)
	}

	if err != nil {
		for _, sm := range lockedSMList {
			sm.UnLockCheckpointState()
		}
	}

	return err
}

func (s *CheckpointSender) unlockCheckpoint() {
	smList := s.smFac.GetSMList()

	for _, sm := range smList {
		sm.UnLockCheckpointState()
	}
}

func (s *CheckpointSender) sendCheckpointFromSM(sm StateMachine) error {
	groupIdx := s.config.GetMyGroupIdx()

	dirPath, files, err := sm.GetCheckpointState(groupIdx)
	if err != nil {
		log.Error("get checkpoint state fail", log.Err(err), log.Int("sm_id", sm.SMID()))
		return err
	}

	if dirPath == "" {
		log.Error("no checkpoint", log.Int("smid", sm.SMID()))
		return nil
	}

	for _, filePath := range files {
		err := s.sendFile(sm, dirPath, filePath)
		if err != nil {
			log.Error("send file fail", log.Err(err), log.Int("smid", sm.SMID()))
			return err
		}
	}

	log.Info("[END] send checkpoint from sm ok",
		log.Int("smid", sm.SMID()), log.Int("files count", len(files)))
	return nil
}

func (s *CheckpointSender) sendFile(sm StateMachine, dirPath string, filePath string) error {
	log.Info("[START] send file", log.Int("smid", sm.SMID()),
		log.String("dirpath", dirPath), log.String("filepath", filePath))

	groupIdx := s.config.GetMyGroupIdx()
	path := filepath.Join(dirPath, filePath)

	if _, ok := s.alreadySentFile[path]; ok {
		log.Error("file already send", log.String("filepath", path))
		return nil
	}

	f, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Error("open file fail", log.String("filepath", path))
		return err
	}
	defer f.Close()

	var (
		readLen int = 0
		offset  int = 0
	)

	for {
		readLen, err = f.Read(s.tmpBuffer)
		if err == io.EOF || readLen == 0 {
			break
		}

		if err != nil || readLen < 0 {
			log.Error("read file fail", log.Err(err), log.Int("read_len", readLen))
			return fmt.Errorf("read fail %v", err)
		}

		err = s.sendBuffer(sm.SMID(), sm.GetCheckpointInstanceID(groupIdx),
			filePath, uint64(offset), string(s.tmpBuffer))
		if err != nil {
			return err
		}

		log.Debug("send ok", log.Int("offset", offset), log.Int("read_len", readLen))

		if readLen < len(s.tmpBuffer) {
			break
		}

		offset += readLen
	}

	s.alreadySentFile[path] = true
	log.Info("[END] send file succ")
	return nil
}

func (s *CheckpointSender) sendBuffer(smID int,
	checkpointInstanceID uint64,
	filePath string,
	offset uint64,
	buffer string) error {

	checksum := Crc32(buffer)
	var err error
	for {
		if s.IsEnd() {
			return errors.New("checkpoint sender end")
		}
		if !s.checkAck(s.sequence) {
			return errors.New("checkpoint sender check ack fail")
		}

		err = s.learner.SendCheckpoint(s.sendNodeID, s.uuid, s.sequence, checkpointInstanceID,
			checksum, filePath, smID, offset, buffer)
		if err == nil {
			s.sequence++
			break
		} else {
			log.Error("send checkpoint fail, need sleep 30s", log.Err(err))
			time.Sleep(30000 * time.Millisecond)
		}
	}

	return err
}

func (s *CheckpointSender) checkAck(sendSequence uint64) bool {
	for sendSequence > s.ackSequence+CheckpointAckLead {
		nowTime := GetCurrentTimeMs()
		passTime := nowTime - s.absLastAckTime
		if passTime < 0 {
			passTime = 0
		}

		if s.isEnd {
			return false
		}

		if passTime >= CheckpointAckTimeout {
			log.Error("act timeout", log.Uint64("last act time", s.absLastAckTime))
			return false
		}

		time.Sleep(20 * time.Millisecond)
	}

	return true
}
