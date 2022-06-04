package paxos

import (
	"errors"
	"github.com/AllenShaw19/paxos/log"
	"os"
	"path/filepath"
)

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

func (l *Learner) NewInstance() {
	l.instanceID++
	l.InitForNewPaxosInstance()
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

	masterVariablesChanged := false
	if l.config.GetMasterSM() != nil {
		masterVariablesChanged, err = l.config.GetMasterSM().UpdateByCheckpoint(msg.MasterVariables)
		if err == nil && masterVariablesChanged {
			log.Info("master variables changed!")
		}
	}

	if msg.InstanceID != l.GetInstanceID() {
		log.Error("lag msg, skip")
		return
	}

	if msg.NowInstanceID <= l.GetInstanceID() {
		log.Error("lag msg, skip")
		return
	}

	if msg.MinChosenInstanceID > l.GetInstanceID() {
		log.Info("my instanceid small than other's min chosen instanceid",
			log.Uint64("my_instance_id", l.GetInstanceID()),
			log.Uint64("other's_min_chosen_instance_id", msg.MinChosenInstanceID),
			log.Uint64("other_nodeid", msg.NodeID))
		l.AskForCheckpoint(NodeID(msg.NodeID))
	} else if !l.isLearning {
		l.ConfirmAskForLearn(NodeID(msg.NodeID))
	}
}

func (l *Learner) AskForCheckpoint(sendNodeID NodeID) {
	log.Info("START")

	err := l.checkpointMgr.PrepareForAskForCheckpoint(sendNodeID)
	if err != nil {
		return
	}

	paxosMsg := &PaxosMsg{}
	paxosMsg.InstanceID = l.GetInstanceID()
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.MsgType = MsgTypePaxosLearnerAskForCheckpoint

	log.Info("END", log.Uint64("instance_id", l.GetInstanceID()),
		log.Uint64("my_nodeid", uint64(l.config.GetMyNodeID())))

	err = l.sendMessage(sendNodeID, paxosMsg)
	if err != nil {
		log.Error("send message fail", log.Err(err))
	}
}

func (l *Learner) OnAskForCheckpoint(msg *PaxosMsg) {
	checkpointSender := l.GetNewCheckpointSender(NodeID(msg.NodeID))
	if checkpointSender != nil {
		checkpointSender.Start()
		log.Info("new checkpoint sender started", log.Uint64("send_to_nodeid", msg.NodeID))
	} else {
		log.Error("checkpoint sender is running")
	}
}

// confirm learn

func (l *Learner) ConfirmAskForLearn(sendNodeID NodeID) {
	log.Info("START")

	paxosMsg := &PaxosMsg{}
	paxosMsg.InstanceID = l.GetInstanceID()
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.MsgType = MsgTypePaxosLearnerComfirmAskForLearn

	log.Info("END", log.Uint64("instance_id", l.GetInstanceID()),
		log.Uint64("my_nodeid", paxosMsg.NodeID))

	err := l.sendMessage(sendNodeID, paxosMsg)
	if err != nil {
		log.Error("send message fail", log.Err(err))
	}
	l.isLearning = true
}

func (l *Learner) OnConfirmAskForLearn(msg *PaxosMsg) {
	log.Info("START", log.Uint64("msg.instanceid", msg.InstanceID),
		log.Uint64("msg.from_nodeid", msg.NodeID))

	if !l.learnerSender.Confirm(msg.InstanceID, NodeID(msg.NodeID)) {
		log.Error("learner sender confirm fail, maybe is lag msg")
		return
	}

	log.Info("ok, confirm success")
}

func (l *Learner) SendLearnValue(sendNodeID NodeID,
	learnInstanceID uint64,
	learnedBallot *BallotNumber,
	learnedValue string,
	checksum uint32,
	needAck bool) error {

	paxosMsg := &PaxosMsg{}
	paxosMsg.MsgType = MsgTypePaxosLearnerSendLearnValue
	paxosMsg.InstanceID = learnInstanceID
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.ProposalNodeID = uint64(learnedBallot.NodeID)
	paxosMsg.ProposalID = learnedBallot.ProposalID
	paxosMsg.Value = []byte(learnedValue)
	paxosMsg.LastChecksum = checksum

	if needAck {
		paxosMsg.Flag = PaxosMsgFlagTypeSendLearnValueNeedAck
	}
	return l.sendMessage(sendNodeID, paxosMsg)
}

func (l *Learner) OnSendLearnValue(msg *PaxosMsg) {
	log.Info("START", log.Uint64("msg.instanceid", msg.InstanceID),
		log.Uint64("now.instanceid", l.GetInstanceID()),
		log.Uint64("msg.ballot_proposal_id", msg.ProposalID),
		log.Uint64("msg.ballot_nodeid", msg.NodeID),
		log.Int("msg.value_size", len(msg.Value)))

	if msg.InstanceID > l.GetInstanceID() {
		log.Warn("[Latest Msg] can learn")
		return
	}

	if msg.InstanceID < l.GetInstanceID() {
		log.Warn("[Lag Msg] no need to learn")
	} else {
		ballot := NewBallotNumber(msg.ProposalID, NodeID(msg.ProposalNodeID))
		err := l.state.LearnValue(msg.InstanceID, ballot, string(msg.Value), l.GetLastChecksum())
		if err != nil {
			log.Error("learnstate.learnvalue fail", log.Err(err))
			return
		}
		log.Info("END learnvalue ok", log.Uint64("proposalid", msg.ProposalID),
			log.Uint64("proposalid_nodeid", msg.NodeID),
			log.Int("valuelen", len(msg.Value)))
	}

	if msg.Flag == PaxosMsgFlagTypeSendLearnValueNeedAck {
		//every time' when receive valid need ack learn value, reset noop timeout.
		l.ResetAskForLearnNoop(AskForLearnNoopInterval())
		l.SendLearnValueAck(NodeID(msg.NodeID))
	}
}

func (l *Learner) SendLearnValueAck(sendNodeID NodeID) {
	log.Info("START", log.Uint64("lastAck.instanceid", l.lastAckInstanceID),
		log.Uint64("now.instanceid", l.GetInstanceID()))

	if l.GetInstanceID() < l.lastAckInstanceID+uint64(LearnerReceiverAckLead()) {
		log.Info("no need to ack")
		return
	}

	l.lastAckInstanceID = l.GetInstanceID()

	paxosMsg := &PaxosMsg{}
	paxosMsg.InstanceID = l.GetInstanceID()
	paxosMsg.MsgType = MsgTypePaxosLearnerSendLearnValueAck
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())

	err := l.sendMessage(sendNodeID, paxosMsg)
	if err != nil {
		log.Error("send message fail", log.Err(err))
	}

	log.Info("END. ok")
}

func (l *Learner) OnSendLearnValueAck(msg *PaxosMsg) {
	log.Info("on send learn value ack",
		log.Uint64("msg.ack.instanceid", msg.InstanceID),
		log.Uint64("msg.from_nodeid", msg.NodeID))

	l.learnerSender.Ack(msg.InstanceID, NodeID(msg.NodeID))
}

// success learn

func (l *Learner) ProposerSendSuccess(learnInstanceID, proposalID uint64) {
	paxosMsg := &PaxosMsg{}
	paxosMsg.MsgType = MsgTypePaxosLearnerProposerSendSuccess
	paxosMsg.InstanceID = learnInstanceID
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.ProposalID = proposalID
	paxosMsg.LastChecksum = l.GetLastChecksum()

	//run self first
	err := l.broadcastMessage(paxosMsg, BroadcastMessageTypeRunSelfFirst)
	if err != nil {
		log.Error("broadcast message fail", log.Err(err))
	}
}

func (l *Learner) OnProposerSendSuccess(msg *PaxosMsg) {
	log.Info("START", log.Uint64("msg.instanceid", msg.InstanceID), log.Uint64("now.instanceid", l.GetInstanceID()),
		log.Uint64("msg.proposalid", msg.ProposalID), log.Uint64("state.acceptedid", l.acceptor.GetAcceptorState().GetAcceptedBallot().ProposalID),
		log.Uint64("state.accepted_nodeid", l.acceptor.GetAcceptorState().GetAcceptedBallot().ProposalID),
		log.Uint64("msg.from_nodeid", msg.NodeID))

	if msg.InstanceID != l.GetInstanceID() {
		//Instance id not same, that means not in the same instance, ignord.
		log.Warn("instanceid not same, skip msg")
		return
	}

	if l.acceptor.GetAcceptorState().GetAcceptedBallot().IsNull() {
		//Not accept any yet.
		log.Warn("not accepted any proposal")
		return
	}

	ballot := NewBallotNumber(msg.ProposalID, NodeID(msg.NodeID))

	if !l.acceptor.GetAcceptorState().GetAcceptedBallot().Equal(ballot) {
		// proposalid not same, this accept value maybe not chosen value.
		log.Warn("proposal ballot not same as accepted ballot")
		return
	}

	l.state.LearnValueWithoutWrite(l.acceptor.GetAcceptorState().GetAcceptedValue(), l.acceptor.GetAcceptorState().Checksum())

	log.Info("END Learn value ok", log.Int("valuelen", len(l.acceptor.GetAcceptorState().GetAcceptedValue())))

	l.TransmitToFollower()
}

func (l *Learner) TransmitToFollower() {
	if l.config.GetMyFollowerCount() == 0 {
		return
	}

	acceptorState := l.acceptor.GetAcceptorState()

	paxosMsg := &PaxosMsg{}
	paxosMsg.MsgType = MsgTypePaxosLearnerSendLearnValue
	paxosMsg.InstanceID = l.GetInstanceID()
	paxosMsg.NodeID = uint64(l.config.GetMyNodeID())
	paxosMsg.ProposalNodeID = uint64(acceptorState.GetAcceptedBallot().NodeID)
	paxosMsg.ProposalID = acceptorState.GetAcceptedBallot().ProposalID
	paxosMsg.Value = []byte(acceptorState.GetAcceptedValue())
	paxosMsg.LastChecksum = l.GetLastChecksum()

	err := l.broadcastMessageToFollower(paxosMsg)
	if err != nil {
		log.Error("broadcast message to follower fail", log.Err(err))
	}

	log.Info("transmit to follower done")
}

// learn loop

func (l *Learner) AskForLearnNoop(isStart bool) {
	l.ResetAskForLearnNoop(AskForLearnNoopInterval())

	l.isLearning = false
	l.checkpointMgr.ExitCheckpointMode()

	l.AskForLearn()
	if isStart {
		l.AskForLearn()
	}
}

func (l *Learner) ResetAskForLearnNoop(timeout int) {
	if l.askForLearnNoopTimerID > 0 {
		l.loop.RemoveTimer(l.askForLearnNoopTimerID)
	}
	l.askForLearnNoopTimerID = l.loop.AddTimer(timeout, TimerLearnerAskForLearnNoop)
}

// checkpoint

func (l *Learner) SendCheckpointBegin(sendNodeID NodeID, uuid, sequence, checkpointInstanceID uint64) error {

	checkpointMsg := &CheckpointMsg{}
	checkpointMsg.MsgType = CheckpointMsgTypeSendFile
	checkpointMsg.NodeID = uint64(l.config.GetMyNodeID())
	checkpointMsg.Flag = CheckpointSendFileFlagBEGIN
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.CheckpointInstanceID = checkpointInstanceID

	log.Info("END", log.Uint64("send_nodeid", uint64(sendNodeID)), log.Uint64("uuid", uuid),
		log.Uint64("sequence", sequence), log.Uint64("checkpoint.instanceid", checkpointInstanceID))

	return l.sendCheckpointMsg(sendNodeID, checkpointMsg)
}

func (l *Learner) SendCheckpoint(sendNodeID NodeID,
	uuid, sequence, checkpointInstanceID uint64,
	checksum uint32, filePath string, smID int,
	offset uint64, buffer string) error {

	checkpointMsg := &CheckpointMsg{}
	checkpointMsg.MsgType = CheckpointMsgTypeSendFile
	checkpointMsg.NodeID = uint64(l.config.GetMyNodeID())
	checkpointMsg.Flag = CheckpointSendFileFlagING
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.CheckpointInstanceID = checkpointInstanceID
	checkpointMsg.Checksum = checksum
	checkpointMsg.FilePath = filePath
	checkpointMsg.SMID = int32(smID)
	checkpointMsg.Offset = offset
	checkpointMsg.Buffer = []byte(buffer)

	log.Info("END", log.Uint64("send_nodeid", uint64(sendNodeID)), log.Uint64("uuid", uuid),
		log.Uint64("sequence", sequence), log.Uint64("checkpoint.instanceid", checkpointInstanceID),
		log.Int("smid", smID), log.Uint64("offset", offset), log.Int("buffsize", len(buffer)),
		log.String("filepath", filePath))

	return l.sendCheckpointMsg(sendNodeID, checkpointMsg)
}

func (l *Learner) SendCheckpointEnd(sendNodeID NodeID, uuid, sequence, checkpointInstanceID uint64) error {
	checkpointMsg := &CheckpointMsg{}
	checkpointMsg.MsgType = CheckpointMsgTypeSendFile
	checkpointMsg.NodeID = uint64(l.config.GetMyNodeID())
	checkpointMsg.Flag = CheckpointSendFileFlagEND
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.CheckpointInstanceID = checkpointInstanceID

	log.Info("EMD", log.Uint64("send_nodeid", uint64(sendNodeID)), log.Uint64("uuid", uuid),
		log.Uint64("sequence", sequence), log.Uint64("checkpoint.instanceid", checkpointInstanceID))

	return l.sendCheckpointMsg(sendNodeID, checkpointMsg)
}

func (l *Learner) OnSendCheckpoint(msg *CheckpointMsg) {
	log.Info("START", log.Any("msg", msg))

	var err error
	if msg.Flag == CheckpointSendFileFlagBEGIN {
		err = l.OnSendCheckpointBegin(msg)
	} else if msg.Flag == CheckpointSendFileFlagING {
		err = l.OnSendCheckpointIng(msg)
	} else if msg.Flag == CheckpointSendFileFlagEND {
		err = l.OnSendCheckpointEnd(msg)
	}

	if err != nil {
		log.Error("[FAIL] reset checkpoint receiver and reset askforlearn")
		l.checkpointReceiver.Reset()
		l.ResetAskForLearnNoop(5000)
		err := l.SendCheckpointAck(NodeID(msg.NodeID), msg.UUID, msg.Sequence, CheckpointSendFileAckFlagFail)
		if err != nil {
			log.Error("send checkpoint ack fail", log.Err(err))
		}
		return
	}

	err = l.SendCheckpointAck(NodeID(msg.NodeID), msg.UUID, msg.Sequence, CheckpointSendFileAckFlagOK)
	if err != nil {
		log.Error("send checkpoint ack fail", log.Err(err))
	}
	l.ResetAskForLearnNoop(120000)
}

func (l *Learner) SendCheckpointAck(sendNodeID NodeID, uuid, sequence uint64, flag int) error {
	checkpointMsg := &CheckpointMsg{}
	checkpointMsg.MsgType = CheckpointMsgTypeSendFileAck
	checkpointMsg.NodeID = uint64(l.config.GetMyNodeID())
	checkpointMsg.Flag = int32(flag)
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence

	return l.sendCheckpointMsg(sendNodeID, checkpointMsg)
}

func (l *Learner) OnSendCheckpointAck(msg *CheckpointMsg) {
	log.Info("START", log.Int32("flag", msg.Flag))

	if l.checkpointSender != nil && !l.checkpointSender.IsEnd() {
		if msg.Flag == CheckpointSendFileAckFlagOK {
			l.checkpointSender.Ack(NodeID(msg.NodeID), msg.UUID, msg.Sequence)
		} else {
			l.checkpointSender.End()
		}
	}
}

func (l *Learner) GetNewCheckpointSender(sendNodeID NodeID) *CheckpointSender {
	if l.checkpointSender != nil {
		if l.checkpointSender.IsEnd() {
			l.checkpointSender.Join()
			l.checkpointSender = nil
		}
	}

	if l.checkpointSender == nil {
		l.checkpointSender = NewCheckpointSender(sendNodeID, l.config, l, l.smFac, l.checkpointMgr)
		return l.checkpointSender
	}

	return nil
}

func (l *Learner) IsLatest() bool {
	return (l.GetInstanceID() + 1) >= l.highestSeenInstanceID
}

func (l *Learner) GetSeenLatestInstanceID() uint64 {
	return l.highestSeenInstanceID
}

func (l *Learner) SetSeenInstanceID(instanceID uint64, fromNodeID NodeID) {
	if instanceID > l.highestSeenInstanceID {
		l.highestSeenInstanceID = instanceID
		l.highestSeenInstanceIDFromNodeID = fromNodeID
	}
}

func (l *Learner) OnSendCheckpointBegin(msg *CheckpointMsg) error {
	err := l.checkpointReceiver.NewReceiver(NodeID(msg.NodeID), msg.UUID)
	if err == nil {
		return err
	}

	log.Info("new receiver ok")
	err = l.checkpointMgr.SetMinChosenInstanceID(msg.CheckpointInstanceID)
	if err != nil {
		log.Error("set min chosen instanceid fail", log.Err(err), log.Uint64("checkpoint.instanceid", msg.CheckpointInstanceID))
		return err
	}
	return nil
}

func (l *Learner) OnSendCheckpointIng(msg *CheckpointMsg) error {
	return l.checkpointReceiver.ReceiveCheckpoint(msg)
}

func (l *Learner) OnSendCheckpointEnd(msg *CheckpointMsg) error {
	if !l.checkpointReceiver.IsReceiverFinish(NodeID(msg.NodeID), msg.UUID, msg.Sequence) {
		log.Error("receive end msg but receiver not finish")
		return errors.New("receive end msg but receiver not finish")
	}

	smList := l.smFac.GetSMList()
	for _, sm := range smList {
		if sm.SMID() == SystemVSMID || sm.SMID() == MasterVSMID {
			//system variables sm no checkpoint
			//master variables sm no checkpoint
			continue
		}

		tmpDirPath := l.checkpointReceiver.GetTmpDirPath(sm.SMID())
		filePaths := make([]string, 0)
		err := filepath.Walk(tmpDirPath, func(path string, info os.FileInfo, err error) error {
			filePaths = append(filePaths, path)
			return nil
		})

		if err != nil {
			log.Error("iter dir fail", log.Err(err), log.String("dirpath", tmpDirPath))
		}
		if len(filePaths) == 0 {
			log.Info("sm have no checkpoint", log.Int("smid", sm.SMID()))
			continue
		}

		err = sm.LoadCheckpointState(l.config.GetMyGroupIdx(), tmpDirPath, filePaths, msg.CheckpointInstanceID)
		if err != nil {
			return err
		}
	}

	log.Info("all sm load state ok, exit process")
	os.Exit(-1)

	return nil
}
