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
	l.checkpointReceiver = NewCheckpointReceiver(config, logStorage)
	l.acceptor = acceptor

	l.InitForNewPaxosInstance()

	l.askForLearnNoopTimerID = 0
	l.loop = loop
	l.checkpointMgr = checkpointMgr
	l.smFac = smFac
	l.checkpointSender = nil
	l.highestSeenInstanceID = 0
	l.highestSeenInstanceIDFromNodeID = nilNode
	l.isLearning = false
	l.lastAckInstanceID = 0

	return l
}

func (l *Learner) Close() {
	l.checkpointSender = nil
}

func (l *Learner) StartLearnerSender() {
	l.learnerSender.Start()
}

func (l *Learner) InitForNewPaxosInstance() {
	l.state.Init()
}

func (l *Learner) IsLearned() bool {
	return l.state.GetIsLearned()
}

func (l *Learner) GetLearnValue() string {
	return l.state.GetLearnValue()
}

func (l *Learner) GetNewChecksum() uint32 {
	return l.state.GetNewChecksum()
}

func (l *Learner) Stop() {
	l.learnerSender.Stop()
	if l.checkpointSender != nil {
		l.checkpointSender.Stop()
	}
}

//  prepare learn

func (l *Learner) AskForLearn() {
	log.Info("START")

	paxosMsg := &PaxosMsg{}
	paxosMsg.InstanceID = l.GetInstanceID()
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.MsgType = MsgTypePaxosLearnerAskForLearn

	if l.config.isFollower {
		//this is not proposal nodeid, just use this val to bring follow to nodeid info.
		paxosMsg.ProposalNodeID = uint64(l.config.GetFollowToNodeID())
	}

	log.Info("END", log.Uint64("instance_id", paxosMsg.InstanceID),
		log.Uint64("my_node_id", paxosMsg.NodeID))

	err := l.broadcastMessage(paxosMsg, BroadcastMessageTypeRunSelfNone)
	if err != nil {
		log.Error("broadcast message fail",
			log.Uint64("instance_id", paxosMsg.InstanceID),
			log.Err(err))
	}
	err = l.broadcastMessageToTempNode(paxosMsg)
	if err != nil {
		log.Error("broadcast message to temp node fail",
			log.Uint64("instance_id", paxosMsg.InstanceID),
			log.Err(err))
	}
}

func (l *Learner) OnAskForLearn(msg *PaxosMsg) {
	log.Info("START", log.Uint64("msg.instance_id", msg.InstanceID),
		log.Uint64("now.instance_id", l.GetInstanceID()),
		log.Uint64("msg.from_nodeid", msg.NodeID),
		log.Uint64("min_chosen_instance_id", l.checkpointMgr.GetMinChosenInstanceID()))

	l.SetSeenInstanceID(msg.InstanceID, NodeID(msg.NodeID))

	if NodeID(msg.ProposalNodeID) == l.config.GetMyNodeID() {
		//Found a node follow me.
		log.Info("found a node follow me", log.Uint64("node_id", msg.NodeID))
		l.config.AddFollowerNode(NodeID(msg.NodeID))
	}

	if msg.InstanceID >= l.GetInstanceID() {
		return
	}

	if msg.InstanceID >= l.checkpointMgr.GetMinChosenInstanceID() {
		if !l.learnerSender.Prepare(msg.InstanceID, NodeID(msg.NodeID)) {
			log.Error("learner sender working for others")

			if msg.InstanceID == l.GetInstanceID()-1 {
				log.Info("instance_id only difference one, just send this value to other.")
				// send one value
				state, err := l.paxosLog.ReadState(l.config.GetMyGroupIdx(), msg.InstanceID)
				if err == nil {
					ballot := NewBallotNumber(state.AcceptedID, NodeID(state.AcceptedNodeID))
					err := l.SendLearnValue(NodeID(msg.NodeID), msg.InstanceID, ballot, string(state.AcceptedValue), 0, false)
					if err != nil {
						log.Error("send learn value fail", log.Err(err))
					}
				}
			}
		}
	}

	l.SendNowInstanceID(msg.InstanceID, NodeID(msg.NodeID))
}

func (l *Learner) SendNowInstanceID(instanceID uint64, sendNodeID NodeID) {
	paxosMsg := &PaxosMsg{}
	paxosMsg.InstanceID = instanceID
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.MsgType = MsgTypePaxosLearnerSendNowInstanceID
	paxosMsg.NowInstanceID = l.GetInstanceID()
	paxosMsg.MinChosenInstanceID = l.checkpointMgr.GetMinChosenInstanceID()

	if l.GetInstanceID()-instanceID > 50 {
		//instanceid too close not need to send vsm/master checkpoint.
		systemVariablesBuff, err := l.config.GetSystemVSM().GetCheckpointBuffer()
		if err == nil {
			paxosMsg.SystemVariables = []byte(systemVariablesBuff)
		}

		if l.config.GetMasterSM() != nil {
			masterVariablesBuff, err := l.config.GetMasterSM().GetCheckpointBuffer()
			if err == nil {
				paxosMsg.MasterVariables = []byte(masterVariablesBuff)
			}
		}
	}

	err := l.sendMessage(sendNodeID, paxosMsg)
	if err != nil {
		log.Error("send message fail", log.Err(err))
	}
}

func (l *Learner) OnSendNowInstanceID(msg *PaxosMsg) {
	log.Info("START",
		log.Uint64("msg.instance_id", msg.InstanceID),
		log.Uint64("now.instance_id", l.GetInstanceID()),
		log.Uint64("msg.from_node_id", msg.NodeID),
		log.Uint64("msg.max_instance_id", msg.NowInstanceID),
		log.Int("system_variables_size", len(msg.SystemVariables)),
		log.Int("master_variables_size", len(msg.MasterVariables)))

	l.SetSeenInstanceID(msg.NowInstanceID, NodeID(msg.NodeID))

	systemVariablesChanged, err := l.config.GetSystemVSM().UpdateByCheckpoint(msg.SystemVariables)
	if err == nil && systemVariablesChanged {
		log.Info("system variables changed!, need to reflesh, skip this msg")
		return
	}

	if l.config.GetMasterSM() != nil {
		l.config.GetMasterSM().UpdateByCheckpoint()
	}

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
