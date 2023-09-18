package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"math"
	"sync"
	"time"
)

type LearnerSender struct {
	config   *Config
	learner  *Learner
	paxosLog *PaxosLog
	lock     sync.Mutex

	isSending       bool
	absLastSendTime uint64

	beginInstanceID uint64
	sendToNodeID    NodeID

	isConfirmed bool

	ackInstanceID  uint64
	absLastAckTime uint64
	ackLead        int

	isEnd   bool
	isStart bool

	doneC chan struct{}
	waitC chan struct{}
}

func NewLearnerSender(config *Config, learner *Learner, paxosLog *PaxosLog) *LearnerSender {
	s := &LearnerSender{}
	s.config = config
	s.learner = learner
	s.paxosLog = paxosLog
	s.ackLead = LearnerSenderAckLead()
	s.isEnd = false
	s.isStart = false
	s.doneC = make(chan struct{})
	s.waitC = make(chan struct{})

	s.sendDone()

	return s
}

func (s *LearnerSender) Run() {
	s.isStart = true
	go s.waitToSend()
	for {
		select {
		case <-s.waitC:
			if s.isEnd {
				log.Info("learner.sender [END]")
				return
			}
			s.sendLearnedValue(s.beginInstanceID, s.sendToNodeID)
			s.sendDone()
		case <-s.doneC:
			log.Info("learner.sender [END]")
			return
		}
	}
}

func (s *LearnerSender) Stop() {
	if s.isStart {
		s.isEnd = true
		close(s.doneC)
	}
}

func (s *LearnerSender) Prepare(beginInstanceID uint64, sendToNode NodeID) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	prepareRet := false
	if !s.IsSending() && !s.isConfirmed {
		prepareRet = true
		s.isSending = true
		s.absLastAckTime = GetCurrentTimeMs()
		s.absLastSendTime = s.absLastAckTime
		s.beginInstanceID = beginInstanceID
		s.ackInstanceID = beginInstanceID
		s.sendToNodeID = sendToNode
	}

	return prepareRet
}

func (s *LearnerSender) Confirm(beginInstanceID uint64, sendToNode NodeID) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	confirmRet := false
	if !s.IsSending() && !s.isConfirmed {
		if s.beginInstanceID == beginInstanceID && s.sendToNodeID == sendToNode {
			confirmRet = true
			s.isConfirmed = true
		}
	}

	return confirmRet
}

func (s *LearnerSender) Ack(ackInstanceID uint64, fromNodeID NodeID) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.IsSending() && !s.isConfirmed {
		if s.sendToNodeID == fromNodeID {
			if ackInstanceID > s.ackInstanceID {
				s.ackInstanceID = ackInstanceID
				s.absLastAckTime = GetCurrentTimeMs()
			}
		}
	}
}

func (s *LearnerSender) waitToSend() {
	for !s.isConfirmed {
		timer := time.NewTimer(1000 * time.Millisecond)
		select {
		case <-timer.C:
			s.waitC <- struct{}{}
		case <-s.doneC:
			return
		}
	}
}

func (s *LearnerSender) sendLearnedValue(beginInstanceID uint64, sendToNode NodeID) {
	log.Info("send learned value", log.Uint64("begin_instance_id", beginInstanceID), log.Uint64("send_to_node", uint64(sendToNode)))

	var (
		err          error
		lastChecksum uint32
	)
	sendInstanceID := beginInstanceID

	sendQps := LearnerSenderSendQps()
	sleepMs := 1
	if sendQps <= 1000 {
		sleepMs = 1000 / sendQps
	}
	sendInterval := 1
	if sendQps > 1000 {
		sendInterval = sendQps/1000 + 1
	}

	log.Debug("start send", log.Int("send_qps", sendQps),
		log.Int("send_interval", sendInterval),
		log.Int("ack_lead", s.ackLead))

	sendCount := 0
	for sendInstanceID < s.learner.GetInstanceID() {
		lastChecksum, err = s.sendOne(sendInstanceID, sendToNode, lastChecksum)
		if err != nil {
			log.Error("send one fail", log.Uint64("send_instance_id", sendInstanceID),
				log.Uint64("send_to_node_id", uint64(sendToNode)), log.Err(err))
			return
		}

		if !s.CheckAck(sendInstanceID) {
			return
		}

		sendCount++
		sendInstanceID++
		s.refreshSending()

		if sendCount >= sendInterval {
			sendQps = 0
			time.Sleep(time.Duration(sleepMs) * time.Microsecond)
		}
	}
	//succ send, reset ack lead.
	s.ackLead = LearnerSenderAckLead()
	log.Info("send done", log.Uint64("send_end_instance_id", sendInstanceID))
}

func (s *LearnerSender) sendOne(sendInstanceID uint64, sendToNodeID NodeID, lastChecksum uint32) (uint32, error) {
	state, err := s.paxosLog.ReadState(s.config.GetMyGroupIdx(), sendInstanceID)
	if err != nil {
		return lastChecksum, err
	}

	ballot := NewBallotNumber(state.AcceptedID, NodeID(state.AcceptedNodeID))
	err = s.learner.SendLearnValue(sendToNodeID, sendInstanceID, ballot, string(state.AcceptedValue), lastChecksum, true)
	if err != nil {
		log.Error("learner.SendLearnValue fail", log.Err(err))
	}

	return state.Checksum, err
}

func (s *LearnerSender) sendDone() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.isSending = false
	s.isConfirmed = false
	s.beginInstanceID = math.MaxUint64
	s.sendToNodeID = nilNode
	s.absLastSendTime = 0

	s.ackInstanceID = 0
	s.absLastAckTime = 0
}

func (s *LearnerSender) IsSending() bool {
	if !s.isSending {
		return false
	}

	nowTime := GetCurrentTimeMs()
	passTime := uint64(0)
	if nowTime > s.absLastSendTime {
		passTime = nowTime - s.absLastSendTime
	}

	if int(passTime) >= LearnerSenderPrepareTimeout() {
		return false
	}

	return true
}

func (s *LearnerSender) refreshSending() {
	s.absLastSendTime = GetCurrentTimeMs()
}

func (s *LearnerSender) CheckAck(sendInstanceID uint64) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if sendInstanceID < s.ackInstanceID {
		s.ackLead = LearnerSenderAckLead()
		log.Info("already catch up", log.Uint64("ack_instance_id", s.ackInstanceID),
			log.Uint64("now_send_instance_id", sendInstanceID))
		return false
	}

	for sendInstanceID > s.ackInstanceID+uint64(s.ackLead) {
		nowTime := GetCurrentTimeMs()
		passTime := uint64(0)
		if nowTime > s.absLastAckTime {
			passTime = nowTime - s.absLastAckTime
		}

		if int(passTime) >= LearnerSenderAckTimeout() {
			log.Error("ack timeout", log.Uint64("last_ack_time", s.absLastAckTime),
				log.Uint64("now_send_instance_id", sendInstanceID))
			s.CutAckLead()
			return false
		}
		//time.Sleep(20 * time.Millisecond)
	}

	return true
}

func (s *LearnerSender) CutAckLead() {
	receiverAckLead := LearnerReceiverAckLead()
	if s.ackLead-receiverAckLead > receiverAckLead {
		s.ackLead = s.ackLead - receiverAckLead
	}
}

func (s *LearnerSender) Start() {
	go s.Run()
}
