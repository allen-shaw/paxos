package paxos

import "github.com/AllenShaw19/paxos/log"

type LearnerState struct {
	learnedValue string
	isLearned    bool
	newChecksum  uint32

	config   *Config
	paxosLog *PaxosLog
}

func NewLearnerState(config *Config, logStorage LogStorage) *LearnerState {
	s := &LearnerState{
		config:   config,
		paxosLog: NewPaxosLog(logStorage),
	}
	s.Init()
	return s
}

func (s *LearnerState) Init() {
	s.learnedValue = ""
	s.isLearned = false
	s.newChecksum = 0
}

func (s *LearnerState) LearnValue(instanceID uint64, learnedBallot *BallotNumber,
	value string, lastChecksum uint32) error {

	if instanceID > 0 && lastChecksum == 0 {
		s.newChecksum = 0
	} else if len(value) > 0 {
		s.newChecksum = Crc32(value)
	}

	state := &AcceptorStateData{}
	state.InstanceID = instanceID
	state.AcceptedValue = []byte(value)
	state.PromiseID = learnedBallot.ProposalID
	state.PromiseNodeID = uint64(learnedBallot.NodeID)
	state.AcceptedID = learnedBallot.ProposalID
	state.AcceptedNodeID = uint64(learnedBallot.NodeID)
	state.Checksum = s.newChecksum

	options := &WriteOptions{Sync: false}

	err := s.paxosLog.WriteState(options, s.config.GetMyGroupIdx(), instanceID, state)
	if err != nil {
		log.Error("paxoslog write state fail", log.Uint64("instance_id", instanceID),
			log.Int("value_size", len(value)), log.Err(err))
		return err
	}

	s.LearnValueWithoutWrite(value, s.newChecksum)
	log.Info("learn value success", log.Uint64("instance_id", instanceID),
		log.Int("value_size", len(value)), log.Uint32("checksum", s.newChecksum))
	return nil
}

func (s *LearnerState) LearnValueWithoutWrite(value string, newChecksum uint32) {
	s.learnedValue = value
	s.isLearned = true
	s.newChecksum = newChecksum
}

func (s *LearnerState) GetLearnValue() string {
	return s.learnedValue
}

func (s *LearnerState) GetIsLearned() bool {
	return s.isLearned
}

func (s *LearnerState) GetNewChecksum() uint32 {
	return s.newChecksum
}

///////////////////////////////////

type Learner struct {
	*base
	state *LearnerState

	acceptor *Acceptor
	paxosLog *PaxosLog

	askForLearnNoopTimerID uint32
	loop                   *Loop

	highestSeenInstanceID           uint64
	highestSeenInstanceIDFromNodeID NodeID

	isLearning        bool
	learnerSender     *LearnerSender
	lastAckInstanceID uint64

	checkpointMgr *CheckpointMgr
	smFac         *SMFac

	checkpointSender   *CheckpointSender
	checkpointReceiver *CheckpointReceiver
}

func NewLearner(config *Config,
	msgTransport MsgTransport,
	instance *Instance,
	acceptor *Acceptor,
	logStorage LogStorage,
	loop *Loop,
	checkpointMgr *CheckpointMgr,
	smFac *SMFac) *Learner {
	l := &Learner{}

	l.base = newBase(config, msgTransport, instance)
	l.state = NewLearnerState(config, logStorage)
	l.paxosLog = NewPaxosLog(logStorage)
	l.learnerSender = NewLearnerSender(config, l, l.paxosLog)
	l.checkpointReceiver = NewCheckpointRe

	return l
}

func (l *Learner) Close() {
	l.checkpointSender = nil
}

func (l *Learner) StartLearnerSender() {

}

func (l *Learner) InitForNewPaxosInstance() {

}

func (l *Learner) IsLearned() bool {

}

func (l *Learner) GetLearnValue() string {

}

func (l *Learner) GetNewChecksum() uint32 {

}

func (l *Learner) Stop() {

}

//  prepare learn

func (l *Learner) AskForLearn() {

}

func (l *Learner) OnAskForLearn(msg *PaxosMsg) {

}

func (l *Learner) SendNowInstanceID(instanceID uint64, sendNode NodeID) {

}

func (l *Learner) OnSendNowInstanceID(msg *PaxosMsg) {

}

func (l *Learner) AskForCheckpoint(SendNodeID NodeID) {

}

func (l *Learner) OnAskForCheckpoint(msg *PaxosMsg) {

}

// confirm learn

func (l *Learner) ConfirmAskForLearn(sendNodeID NodeID) {

}

func (l *Learner) OnConfirmAskForLearn(msg *PaxosMsg) {

}

func (l *Learner) SendLearnValue(sendNodeID NodeID,
	learnInstanceID uint64,
	learnedBallot *BallotNumber,
	learnedValue string,
	checksum uint32,
	needAck bool) error {

}

func (l *Learner) OnSendLearnValue(msg *PaxosMsg) {

}

func (l *Learner) SendLearnValueAck(sendNodeID NodeID) {

}

func (l *Learner) OnSendLearnValueAck(msg *PaxosMsg) {

}

// success learn

func (l *Learner) ProposerSendSuccess(learnInstanceID, proposalID uint64) {

}

func (l *Learner) OnProposerSendSuccess(msg *PaxosMsg) {

}

func (l *Learner) TransmitToFollower() {

}

// learn loop

func (l *Learner) AskForLearnNoop(isStart bool) {

}

func (l *Learner) ResetAskForLearnNoop(timeout int) {

}

// checkpoint

func (l *Learner) SendCheckpointBegin(sendNodeID NodeID,
	UUID, sequence, checkpointInstanceID uint64) error {

}

func (l *Learner) SendCheckpoint(sendNodeID NodeID,
	UUID, sequence, checkpointInstanceID uint64,
	checksum uint32, filePath string, smID int,
	offset uint64, buffer string) error {

}

func (l *Learner) SendCheckpointEnd(sendNodeID NodeID,
	UUID, sequence, checkpointInstanceID uint64) error {

}

func (l *Learner) OnSendCheckpoint(msg *CheckpointMsg) {

}

func (l *Learner) SendCheckpointAck(sendNodeID NodeID,
	UUID, sequence uint64, flag int) error {

}

func (l *Learner) OnSendCheckpointAck(msg *CheckpointMsg) {

}

func (l *Learner) GetNewCheckpointSender(sendNodeID NodeID) *CheckpointSender {

}

func (l *Learner) IsLatest() bool {

}

func (l *Learner) GetSeenLatestInstanceID() uint64 {

}

func (l *Learner) SetSeenInstanceID(instanceID uint64, fromNodeID NodeID) {

}

func (l *Learner) OnSendCheckpointBegin(msg *CheckpointMsg) error {

}

func (l *Learner) OnSendCheckpointIng(msg *CheckpointMsg) error {

}

func (l *Learner) OnSendCheckpointEnd(msg *CheckpointMsg) error {

}
