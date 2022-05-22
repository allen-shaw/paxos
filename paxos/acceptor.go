package paxos

import "github.com/AllenShaw19/paxos/log"

type AcceptorState struct {
	promiseBallot  *BallotNumber
	acceptedBallot *BallotNumber
	acceptedValue  string
	checksum       uint32

	config   *Config
	paxosLog *PaxosLog

	syncTimes int
}

func NewAcceptorState(config *Config, logStorage LogStorage) *AcceptorState {
	as := &AcceptorState{
		promiseBallot:  &BallotNumber{},
		acceptedBallot: &BallotNumber{},
		config:         config,
		paxosLog:       NewPaxosLog(logStorage),
		syncTimes:      0,
	}
	as.Init()
	return as
}

func (s *AcceptorState) Init() {
	s.acceptedBallot.Reset()
	s.acceptedValue = ""
	s.checksum = 0
}

func (s *AcceptorState) GetPromiseBallot() *BallotNumber {
	return s.promiseBallot
}

func (s *AcceptorState) SetPromiseBallot(promiseBallot *BallotNumber) {
	s.promiseBallot = promiseBallot
}

func (s *AcceptorState) GetAcceptedBallot() *BallotNumber {
	return s.acceptedBallot
}

func (s *AcceptorState) SetAcceptedBallot(acceptedBallot *BallotNumber) {
	s.acceptedBallot = acceptedBallot
}

func (s *AcceptorState) GetAcceptedValue() string {
	return s.acceptedValue
}

func (s *AcceptorState) SetAcceptedValue(acceptedValue string) {
	s.acceptedValue = acceptedValue
}

func (s *AcceptorState) Checksum() uint32 {
	return s.checksum
}

func (s *AcceptorState) Persist(instanceID uint64, lastChecksum uint32) error {
	if instanceID > 0 && lastChecksum == 0 {
		s.checksum = 0
	} else if s.acceptedValue != "" {
		s.checksum = Crc32(s.acceptedValue)
	}

	state := &AcceptorStateData{
		InstanceID:     instanceID,
		PromiseID:      s.promiseBallot.ProposalID,
		PromiseNodeID:  uint64(s.promiseBallot.NodeID),
		AcceptedID:     s.acceptedBallot.ProposalID,
		AcceptedNodeID: uint64(s.promiseBallot.NodeID),
		AcceptedValue:  []byte(s.acceptedValue),
		Checksum:       s.checksum,
	}

	options := &WriteOptions{}
	options.Sync = s.config.LogSync()

	if options.Sync {
		s.syncTimes++
		if s.syncTimes > s.config.SyncInterval() {
			s.syncTimes = 0
		} else {
			options.Sync = false
		}
	}

	err := s.paxosLog.WriteState(options, s.config.GetMyGroupIdx(), instanceID, state)
	if err != nil {
		return err
	}

	log.Info("persist succ", log.Int("group_idx", s.config.GetMyGroupIdx()),
		log.Uint64("instance_id", instanceID),
		log.Uint64("promise_id", s.promiseBallot.ProposalID),
		log.Uint64("promise_nodeid", uint64(s.promiseBallot.NodeID)),
		log.Uint64("accepted_id", s.acceptedBallot.ProposalID),
		log.Uint64("accepted_nodeid", uint64(s.acceptedBallot.NodeID)),
		log.Int("value_len", len(s.acceptedValue)),
		log.Uint32("checksum", s.checksum))
	return nil
}

func (s *AcceptorState) Load() (uint64, error) {
	groupIdx := s.config.GetMyGroupIdx()
	instanceID, err := s.paxosLog.GetMaxInstanceIDFromLog(groupIdx)
	if err != nil {
		log.Error("load max instance id fail", log.Err(err))
		return 0, err
	}

	if err == ErrNotExist {
		log.Error("empty database")
		return 0, nil
	}

	state, err := s.paxosLog.ReadState(groupIdx, instanceID)
	if err != nil {
		return instanceID, err
	}

	s.promiseBallot.ProposalID = state.PromiseID
	s.promiseBallot.NodeID = NodeID(state.PromiseNodeID)
	s.acceptedBallot.ProposalID = state.AcceptedID
	s.acceptedBallot.NodeID = NodeID(state.AcceptedNodeID)
	s.acceptedValue = string(state.AcceptedValue)
	s.checksum = state.Checksum

	log.Info("load succ", log.Int("group_idx", groupIdx),
		log.Uint64("instance_id", instanceID),
		log.Uint64("promise_id", s.promiseBallot.ProposalID),
		log.Uint64("promise_nodeid", uint64(s.promiseBallot.NodeID)),
		log.Uint64("accepted_id", s.acceptedBallot.ProposalID),
		log.Uint64("accepted_nodeid", uint64(s.acceptedBallot.NodeID)),
		log.Int("value_len", len(s.acceptedValue)))
	return instanceID, nil
}

/////////////////////////////////////////

type Acceptor struct {
	*base
	state *AcceptorState
}

func NewAcceptor(config *Config,
	msgTransport MsgTransport,
	instance *Instance,
	logStorage LogStorage) *Acceptor {
	a := &Acceptor{}

	a.base = newBase(config, msgTransport, instance)
	a.state = NewAcceptorState(config, logStorage)
	return a
}

func (a *Acceptor) Init() error {
	instanceID, err := a.state.Load()
	if err != nil {
		log.Error("load state fail", log.Err(err))
		return err
	}

	if instanceID == 0 {
		log.Warn("empty database")
	}

	a.SetInstanceID(instanceID)
	log.Info("acceptor init succ")
	return nil
}

func (a *Acceptor) InitForNewPaxosInstance() {
	a.state.Init()
}

func (a *Acceptor) GetAcceptorState() *AcceptorState {
	return a.state
}

func (a *Acceptor) OnPrepare(msg *PaxosMsg) error {
	log.Info("START Prepare", log.Uint64("msg.instance_id", msg.InstanceID),
		log.Uint64("msg.from_node_id", msg.NodeID),
		log.Uint64("msg.proposal_id", msg.ProposalID))

	replyPaxosMsg := &PaxosMsg{}
	replyPaxosMsg.InstanceID = a.GetInstanceID()
	replyPaxosMsg.NodeID = uint64(a.config.GetMyNodeID())
	replyPaxosMsg.ProposalID = msg.ProposalID
	replyPaxosMsg.MsgType = MsgTypePaxosPrepareReply

	ballot := NewBallotNumber(msg.ProposalID, NodeID(msg.NodeID))

	if ballot.LargeOrEqual(a.state.GetPromiseBallot()) {
		log.Info("[Promise]",
			log.Uint64("state.promise_id", a.state.GetPromiseBallot().ProposalID),
			log.Uint64("state.promise_nodeid", uint64(a.state.GetPromiseBallot().NodeID)),
			log.Uint64("state.pre_accepted_id", a.state.GetAcceptedBallot().ProposalID),
			log.Uint64("state.pre_accepted_nodeid", uint64(a.state.GetAcceptedBallot().NodeID)),
		)

		replyPaxosMsg.PreAcceptID = a.state.GetAcceptedBallot().ProposalID
		replyPaxosMsg.PreAcceptNodeID = uint64(a.state.GetAcceptedBallot().NodeID)

		if a.state.GetAcceptedBallot().ProposalID > 0 {
			replyPaxosMsg.Value = []byte(a.state.GetAcceptedValue())
		}

		a.state.SetPromiseBallot(ballot)

		err := a.state.Persist(a.GetInstanceID(), a.GetLastChecksum())
		if err != nil {
			log.Error("acceptor_state persist fail", log.Uint64("instance_id", a.GetInstanceID()), log.Err(err))
			return err
		}
	} else {
		log.Info("[Reject]",
			log.Uint64("state.promise_id", a.state.GetPromiseBallot().ProposalID),
			log.Uint64("state.promise_nodeid", uint64(a.state.GetPromiseBallot().NodeID)))

		replyPaxosMsg.RejectByPromiseID = a.state.GetPromiseBallot().ProposalID
	}

	replyNodeID := NodeID(msg.NodeID)
	log.Info("prepare end",
		log.Uint64("now_instance_id", a.GetInstanceID()),
		log.Uint64("reply_nodeid", msg.NodeID))

	err := a.sendMessage(replyNodeID, replyPaxosMsg)
	if err != nil {
		log.Error("send message fail", log.Uint64("to_nodeid", uint64(replyNodeID)), log.Err(err))
		return err
	}

	return nil
}

func (a *Acceptor) OnAccept(msg *PaxosMsg) error {
	log.Info("START Accept", log.Uint64("msg.instance_id", msg.InstanceID),
		log.Uint64("msg.from_node_id", msg.NodeID),
		log.Uint64("msg.proposal_id", msg.ProposalID),
		log.Int("msg.value_len", len(msg.Value)))

	replyPaxosMsg := &PaxosMsg{}
	replyPaxosMsg.InstanceID = a.GetInstanceID()
	replyPaxosMsg.NodeID = uint64(a.config.GetMyNodeID())
	replyPaxosMsg.ProposalID = msg.ProposalID
	replyPaxosMsg.MsgType = MsgTypePaxosAcceptReply

	ballot := NewBallotNumber(msg.ProposalID, NodeID(msg.NodeID))

	if ballot.LargeOrEqual(a.state.GetPromiseBallot()) {
		log.Info("[Promise]",
			log.Uint64("state.promise_id", a.state.GetPromiseBallot().ProposalID),
			log.Uint64("state.promise_nodeid", uint64(a.state.GetPromiseBallot().NodeID)),
			log.Uint64("state.pre_accepted_id", a.state.GetAcceptedBallot().ProposalID),
			log.Uint64("state.pre_accepted_nodeid", uint64(a.state.GetAcceptedBallot().NodeID)),
		)

		a.state.SetPromiseBallot(ballot)
		a.state.SetAcceptedBallot(ballot)
		a.state.SetAcceptedValue(string(msg.Value))

		err := a.state.Persist(a.GetInstanceID(), a.GetLastChecksum())
		if err != nil {
			log.Error("acceptor_state persist fail", log.Uint64("instance_id", a.GetInstanceID()), log.Err(err))
			return err
		}
	} else {
		log.Info("[Reject]",
			log.Uint64("state.promise_id", a.state.GetPromiseBallot().ProposalID),
			log.Uint64("state.promise_nodeid", uint64(a.state.GetPromiseBallot().NodeID)))

		replyPaxosMsg.RejectByPromiseID = a.state.GetPromiseBallot().ProposalID
	}

	replyNodeID := NodeID(msg.NodeID)
	log.Info("prepare end",
		log.Uint64("now_instance_id", a.GetInstanceID()),
		log.Uint64("reply_nodeid", msg.NodeID))

	err := a.sendMessage(replyNodeID, replyPaxosMsg)
	if err != nil {
		log.Error("send message fail", log.Uint64("to_nodeid", uint64(replyNodeID)), log.Err(err))
		return err
	}

	return nil
}
