package paxos

import "math/rand"

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
	sequence uint

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
	for ;s.checkpointMgr
}

func (s *CheckpointSender) End() {
	s.isEnd = true
}

func (s *CheckpointSender) IsEnd() bool {
	return s.isEnded
}

func (s *CheckpointSender) Ack(sendNodeID NodeID, uuid, sequence uint64) {

}

func (s *CheckpointSender) sendCheckpoint() {

}

func (s *CheckpointSender) lockCheckpoint() error {

}

func (s *CheckpointSender) unlockCheckpoint() {

}

func (s *CheckpointSender) sendCheckpointSM(sm StateMachine) error {

}

func (s *CheckpointSender) sendFile(sm StateMachine, dirPath string, filePath string) error {

}

func (s *CheckpointSender) sendBuffer(smID int, checkpointInstanceID uint64,
	filePath string, offset uint64, buffer string) error {

}

func (s *CheckpointSender) checkAck(sendSequence uint64) bool {

}
