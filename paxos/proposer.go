package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"math/rand"
)

type ProposerState struct {
	proposalID uint64
	value      string

	highestOtherProposalID      uint64
	highestOtherPreAcceptBallot *BallotNumber

	config *Config
}

func NewProposerState(config *Config) *ProposerState {
	s := &ProposerState{config: config, proposalID: 1}
	s.Init()
	return s
}

func (s *ProposerState) Init() {
	s.highestOtherProposalID = 0
	s.value = ""
}

func (s *ProposerState) SetStartProposalID(proposalID uint64) {
	s.proposalID = proposalID
}

func (s *ProposerState) NewPrepare() {
	log.Info("START NewPrepare",
		log.Uint64("proposal_id", s.proposalID),
		log.Uint64("highest_other_proposal_id", s.highestOtherProposalID),
		log.Uint64("my_nodeid", uint64(s.config.GetMyNodeID())))

	maxProposalID := s.highestOtherProposalID
	if s.proposalID > s.highestOtherProposalID {
		maxProposalID = s.proposalID
	}

	s.proposalID = maxProposalID - 1
	log.Info("END NewPrepare", log.Uint64("new.proposal_id", s.proposalID))
}

func (s *ProposerState) AddPreAcceptValue(otherPreAcceptBallot *BallotNumber, otherPreAcceptValue string) {
	log.Info("add pre accept value",
		log.Uint64("other_pre_accept_id", otherPreAcceptBallot.ProposalID),
		log.Uint64("other_pre_accept_nodeid", uint64(otherPreAcceptBallot.NodeID)),
		log.Uint64("highest_other_pre_accept_id", s.highestOtherPreAcceptBallot.ProposalID),
		log.Uint64("highest_other_pre_accept_nodeid", uint64(s.highestOtherPreAcceptBallot.NodeID)),
		log.Int("other_pre_accept_value_size", len(otherPreAcceptValue)))

	if otherPreAcceptBallot.IsNull() {
		return
	}

	if otherPreAcceptBallot.Large(s.highestOtherPreAcceptBallot) {
		s.highestOtherPreAcceptBallot = otherPreAcceptBallot
		s.value = otherPreAcceptValue
	}
}

func (s *ProposerState) GetProposalID() uint64 {
	return s.proposalID
}

func (s *ProposerState) GetValue() string {
	return s.value
}

func (s *ProposerState) SetValue(value string) {
	s.value = value
}

func (s *ProposerState) SetOtherProposalID(otherProposalID uint64) {
	if otherProposalID > s.highestOtherProposalID {
		s.highestOtherProposalID = otherProposalID
	}
}

func (s *ProposerState) ResetHighestOtherPreAcceptBallot() {
	s.highestOtherPreAcceptBallot.Reset()
}

///////////////////////////////////////////

type Proposer struct {
	*base

	state      *ProposerState
	msgCounter *MsgCounter
	learner    *Learner

	isPreparing bool
	isAccepting bool

	loop                 *Loop
	prepareTimerID       uint32
	lastPrepareTimeoutMs int
	acceptTimerID        uint32
	lastAcceptTimeoutMs  int
	timeoutInstanceID    uint64

	canSkipPrepare    bool
	isRejectBySomeone bool
}

func NewProposer(config *Config,
	msgTransport MsgTransport,
	instance *Instance,
	learner *Learner,
	loop *Loop) *Proposer {

	p := &Proposer{}
	p.base = newBase(config, msgTransport, instance)
	p.state = NewProposerState(config)
	p.msgCounter = NewMsgCounter(config)
	p.learner = learner
	p.loop = loop

	p.isPreparing = false
	p.isAccepting = false
	p.canSkipPrepare = false

	p.InitForNewPaxosInstance()

	p.prepareTimerID = 0
	p.acceptTimerID = 0
	p.timeoutInstanceID = 0

	p.lastPrepareTimeoutMs = config.GetPrepareTimeoutMs()
	p.lastAcceptTimeoutMs = config.GetAcceptTimeoutMs()
	p.isRejectBySomeone = false

	return p
}

func (p *Proposer) SetStartProposalID(proposalID uint64) {
	p.state.SetStartProposalID(proposalID)
}

func (p *Proposer) InitForNewPaxosInstance() {
	p.msgCounter.StartNewRound()
	p.state.Init()

	p.ExitPrepare()
	p.ExitAccept()
}

func (p *Proposer) NewValue(value string) error {
	if p.state.GetValue() == "" {
		p.state.SetValue(value)
	}

	p.lastPrepareTimeoutMs = StartPrepareTimeoutMs()
	p.lastAcceptTimeoutMs = StartAcceptTimeoutMs()

	if p.canSkipPrepare && !p.isRejectBySomeone {
		log.Info("skip prepare, directly start accept")
		p.Accept()
	} else {
		//if not reject by someone, no need to increase ballot
		p.Prepare(p.isRejectBySomeone)
	}
	return nil
}

func (p *Proposer) IsWorking() bool {
	return p.isPreparing || p.isAccepting
}

func (p *Proposer) Prepare(needNewBallot bool) {
	log.Info("START Prepare.",
		log.Uint64("instance_id", p.GetInstanceID()),
		log.Uint64("my_nodeid", uint64(p.config.GetMyNodeID())),
		log.Uint64("state.proposal_id", p.state.GetProposalID()),
		log.Int("value_size", len(p.state.GetValue())))

	p.ExitAccept()
	p.isPreparing = true
	p.canSkipPrepare = false
	p.isRejectBySomeone = false

	p.state.ResetHighestOtherPreAcceptBallot()
	if needNewBallot {
		p.state.NewPrepare()
	}

	paxosMsg := &PaxosMsg{}
	paxosMsg.MsgType = MsgTypePaxosPrepare
	paxosMsg.InstanceID = p.GetInstanceID()
	paxosMsg.NodeID = uint64(p.config.GetMyNodeID())
	paxosMsg.ProposalID = p.state.GetProposalID()

	p.msgCounter.StartNewRound()

	p.AddPrepareTimer(0)

	log.Info("END Prepare")

	err := p.broadcastMessage(paxosMsg, BroadcastMessageTypeRunSelfFirst)
	if err != nil {
		log.Error("broadcast message fail", log.Err(err))
		return
	}
}

func (p *Proposer) OnPrepareReply(msg *PaxosMsg) {
	log.Info("START OnPrepareReply.",
		log.Uint64("msg.proposal_id", msg.ProposalID),
		log.Uint64("state.proposal_id", p.state.GetProposalID()),
		log.Uint64("msg.from_nodeid", msg.NodeID),
		log.Uint64("reject_by_promise_id", msg.RejectByPromiseID),
	)

	if p.isPreparing {
		log.Warn("not preparing. skip")
		return
	}

	if msg.ProposalID != p.state.GetProposalID() {
		log.Warn("proposal_id not same. skip")
		return
	}

	p.msgCounter.AddReceive(NodeID(msg.NodeID))

	if msg.RejectByPromiseID == 0 {
		ballot := NewBallotNumber(msg.PreAcceptID, NodeID(msg.PreAcceptNodeID))

		log.Info("[Promise]",
			log.Uint64("pre_accepted_id", msg.PreAcceptID),
			log.Uint64("pre_accepted_nodeid", msg.PreAcceptNodeID),
			log.Int("value_size", len(msg.Value)))

		p.msgCounter.AddPromiseOrAccept(NodeID(msg.NodeID))
		p.state.AddPreAcceptValue(ballot, string(msg.Value))
	} else {
		log.Info("[Reject]", log.Uint64("reject_by_promise_id", msg.RejectByPromiseID))
		p.isRejectBySomeone = true
		p.msgCounter.AddReject(NodeID(msg.NodeID))
		p.state.SetOtherProposalID(msg.RejectByPromiseID)
	}

	if p.msgCounter.IsPassedOnThisRound() {
		log.Info("[Pass] start accept")
		p.canSkipPrepare = true
		p.Accept()
	} else if p.msgCounter.IsRejectedOnThisRound() || p.msgCounter.IsAllReceiveOnThisRound() {
		log.Info("[Not Pass] wait 30ms and restart prepare")
		p.AddPrepareTimer(rand.Intn(30) + 10)
	}

	log.Info("END OnPrepareReply")
}

func (p *Proposer) OnExpiredPrepareReply(msg *PaxosMsg) {
	if msg.RejectByPromiseID != 0 {
		log.Info("[Expired Prepare Reply Reject]", log.Uint64("reject_by_promise_id", msg.RejectByPromiseID))
		p.isRejectBySomeone = true
		p.state.SetOtherProposalID(msg.RejectByPromiseID)
	}
}

func (p *Proposer) Accept() {
	log.Info("START Accept",
		log.Uint64("proposal_id", p.state.GetProposalID()),
		log.Int("value_size", len(p.state.GetValue())))

	p.ExitPrepare()
	p.isAccepting = true

	paxosMsg := &PaxosMsg{}
	paxosMsg.MsgType = MsgTypePaxosAccept
	paxosMsg.InstanceID = p.GetInstanceID()
	paxosMsg.NodeID = uint64(p.config.GetMyNodeID())
	paxosMsg.ProposalID = p.state.GetProposalID()
	paxosMsg.Value = []byte(p.state.GetValue())
	paxosMsg.LastChecksum = p.GetLastChecksum()

	p.msgCounter.StartNewRound()

	p.AddAcceptTimer(0)

	log.Info("END Accept")

	err := p.broadcastMessage(paxosMsg, BroadcastMessageTypeRunSelfFinal)
	if err != nil {
		log.Error("broadcast message fail", log.Err(err))
		return
	}
}

func (p *Proposer) OnAcceptReply(msg *PaxosMsg) {
	log.Info("START OnAcceptReply.",
		log.Uint64("msg.proposal_id", msg.ProposalID),
		log.Uint64("state.proposal_id", p.state.GetProposalID()),
		log.Uint64("msg.from_nodeid", msg.NodeID),
		log.Uint64("reject_by_promise_id", msg.RejectByPromiseID),
	)

	if !p.isAccepting {
		log.Warn("not accepting. skip")
		return
	}

	if msg.ProposalID != p.state.GetProposalID() {
		log.Warn("proposal_id not same. skip")
		return
	}

	p.msgCounter.AddReceive(NodeID(msg.NodeID))

	if msg.RejectByPromiseID == 0 {
		log.Info("[Accept]")
		p.msgCounter.AddPromiseOrAccept(NodeID(msg.NodeID))
	} else {
		log.Info("[Reject]", log.Uint64("reject_by_promise_id", msg.RejectByPromiseID))
		p.isRejectBySomeone = true
		p.msgCounter.AddReject(NodeID(msg.NodeID))
		p.state.SetOtherProposalID(msg.RejectByPromiseID)
	}

	if p.msgCounter.IsPassedOnThisRound() {
		log.Info("[Pass] start send learn")
		p.ExitAccept()
		p.learner.ProposerSendSuccess(p.GetInstanceID(), p.state.GetProposalID())
	} else if p.msgCounter.IsRejectedOnThisRound() || p.msgCounter.IsAllReceiveOnThisRound() {
		log.Info("[Not Pass] wait 30ms and restart prepare")
		p.AddAcceptTimer(rand.Intn(30) + 10)
	}

	log.Info("END OnAcceptReply")
}

func (p *Proposer) OnExpiredAcceptReply(msg *PaxosMsg) {
	if msg.RejectByPromiseID != 0 {
		log.Info("[Expired Accept Reply Reject]", log.Uint64("reject_by_promise_id", msg.RejectByPromiseID))
		p.isRejectBySomeone = true
		p.state.SetOtherProposalID(msg.RejectByPromiseID)
	}
}

func (p *Proposer) OnPrepareTimeout() {
	log.Info("prepare timeout")

	if p.GetInstanceID() != p.timeoutInstanceID {
		log.Error("timeout instance not current instance, skip",
			log.Uint64("timeout_instance_id", p.timeoutInstanceID),
			log.Uint64("current_instance_id", p.GetInstanceID()))
		return
	}

	p.Prepare(p.isRejectBySomeone)
}

func (p *Proposer) OnAcceptTimeout() {
	log.Info("accept timeout")

	if p.GetInstanceID() != p.timeoutInstanceID {
		log.Error("timeout instance not current instance, skip",
			log.Uint64("timeout_instance_id", p.timeoutInstanceID),
			log.Uint64("current_instance_id", p.GetInstanceID()))
		return
	}

	p.Prepare(p.isRejectBySomeone)
}

func (p *Proposer) ExitPrepare() {
	if p.isPreparing {
		p.isPreparing = false
		p.loop.RemoveTimer(p.prepareTimerID)
	}
}

func (p *Proposer) ExitAccept() {
	if p.isAccepting {
		p.isAccepting = false
		p.loop.RemoveTimer(p.acceptTimerID)
	}
}

func (p *Proposer) CancelSkipPrepare() {
	p.canSkipPrepare = false
}

func (p *Proposer) AddPrepareTimer(timeoutMs int) {
	if p.prepareTimerID > 0 {
		p.loop.RemoveTimer(p.prepareTimerID)
	}

	if timeoutMs > 0 {
		p.prepareTimerID = p.loop.AddTimer(timeoutMs, TimerProposerPrepareTimeout)
		return
	}

	p.prepareTimerID = p.loop.AddTimer(p.lastPrepareTimeoutMs, TimerProposerPrepareTimeout)
	p.timeoutInstanceID = p.GetInstanceID()

	log.Info("add prepare timer", log.Int("timeout_ms", p.lastPrepareTimeoutMs))

	p.lastPrepareTimeoutMs *= 2
	if p.lastPrepareTimeoutMs > MaxPrepareTimeoutMs() {
		p.lastPrepareTimeoutMs = MaxPrepareTimeoutMs()
	}
}

func (p *Proposer) AddAcceptTimer(timeoutMs int) {
	if p.acceptTimerID > 0 {
		p.loop.RemoveTimer(p.acceptTimerID)
	}

	if timeoutMs > 0 {
		p.acceptTimerID = p.loop.AddTimer(timeoutMs, TimerProposerAcceptTimeout)
		return
	}

	p.acceptTimerID = p.loop.AddTimer(p.lastAcceptTimeoutMs, TimerProposerAcceptTimeout)
	p.timeoutInstanceID = p.GetInstanceID()

	log.Info("add accept timer", log.Int("timeout_ms", p.lastPrepareTimeoutMs))

	p.lastAcceptTimeoutMs *= 2
	if p.lastAcceptTimeoutMs > MaxAcceptTimeoutMs() {
		p.lastAcceptTimeoutMs = MaxAcceptTimeoutMs()
	}
}
