package paxos

import (
	"encoding/binary"
	"errors"
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
)

type Instance struct {
	config       *Config
	msgTransport MsgTransport

	smFac *SMFac

	loop *Loop

	acceptor *Acceptor
	learner  *Learner
	proposer *Proposer

	paxosLog     *PaxosLog
	lastChecksum uint32

	commitCtx     *CommitCtx
	commitTimerID uint32
	committer     *Committer

	checkpointMgr *CheckpointMgr

	options *Options
	started bool
}

func NewInstance(config *Config,
	logStorage LogStorage,
	msgTransport MsgTransport,
	options *Options) *Instance {
	i := &Instance{}

	i.config = config
	i.msgTransport = msgTransport
	i.commitTimerID = 0
	i.smFac = NewSMFac(config.GetMyGroupIdx())
	i.loop = NewLoop(config, i)
	i.checkpointMgr = NewCheckpointMgr(config, i.smFac, logStorage, options.UseCheckpointReplayer)
	i.acceptor = NewAcceptor(config, msgTransport, i, logStorage)
	i.learner = NewLearner(config, msgTransport, i, i.acceptor, logStorage, i.loop, i.checkpointMgr, i.smFac)
	i.proposer = NewProposer(config, msgTransport, i, i.learner, i.loop)
	i.paxosLog = NewPaxosLog(logStorage)
	i.lastChecksum = 0
	i.commitCtx = NewCommitCtx(config)
	i.commitTimerID = 0
	i.committer = NewCommitter(config, i.commitCtx, i.loop, i.smFac)
	i.checkpointMgr = NewCheckpointMgr(config, i.smFac, logStorage, options.UseCheckpointReplayer)
	i.options = options
	i.started = false

	return i
}

func (i *Instance) Init() error {
	//Must init acceptor first, because the max instanceid is record in acceptor state.
	err := i.acceptor.Init()
	if err != nil {
		log.Error("acceptor.init fail", log.Err(err))
		return err
	}

	err = i.checkpointMgr.Init()
	if err != nil {
		log.Error("checkpointMgr.init fail", log.Err(err))
		return err
	}

	checkpointInstanceID := i.checkpointMgr.GetCheckpointInstanceID() + 1

	log.Info("acceptor.ok", log.Uint64("log.instanceid", i.acceptor.GetInstanceID()),
		log.Uint64("checkpoint.instanceid", checkpointInstanceID))

	nowInstanceID := checkpointInstanceID
	if nowInstanceID < i.acceptor.GetInstanceID() {
		err = i.playLog(nowInstanceID, i.acceptor.GetInstanceID())
		if err != nil {
			return err
		}
		log.Info("playlog ok", log.Uint64("begin_instanceid", nowInstanceID),
			log.Uint64("end_instanceid", i.acceptor.GetInstanceID()))
		nowInstanceID = i.acceptor.GetInstanceID()
	} else {
		if nowInstanceID > i.acceptor.GetInstanceID() {
			err := i.isCheckpointInstanceIDCorrect(nowInstanceID, i.acceptor.GetInstanceID())
			if err != nil {
				return err
			}
			i.acceptor.InitForNewPaxosInstance()
		}
		i.acceptor.SetInstanceID(nowInstanceID)
	}

	log.Info("init", log.Uint64("now.instanceid", nowInstanceID))

	i.learner.SetInstanceID(nowInstanceID)
	i.proposer.SetInstanceID(nowInstanceID)
	i.proposer.SetStartProposalID(i.acceptor.GetAcceptorState().GetPromiseBallot().ProposalID + 1)

	err = i.InitLastCheckSum()
	if err != nil {
		return err
	}

	i.learner.ResetAskForLearnNoop(AskForLearnNoopInterval())

	log.Info("ok")
	return nil
}

func (i *Instance) Start() {
	//start learner sender
	i.learner.StartLearnerSender()
	//start loop
	i.loop.Start()
	//start checkpoint replayer and cleaner
	i.checkpointMgr.Start()

	i.started = true
}

func (i *Instance) Stop() {
	if i.started {
		i.loop.Stop()
		i.checkpointMgr.Stop()
		i.learner.Stop()
	}
}

func (i *Instance) InitLastCheckSum() error {
	if i.acceptor.GetInstanceID() == 0 {
		i.lastChecksum = 0
		return nil
	}

	if i.acceptor.GetInstanceID() <= i.checkpointMgr.GetMinChosenInstanceID() {
		i.lastChecksum = 0
		return nil
	}

	state, err := i.paxosLog.ReadState(i.config.GetMyGroupIdx(), i.acceptor.GetInstanceID())
	if err != nil && err != ErrNotExist {
		return err
	}

	if err == ErrNotExist {
		log.Error("las checksum not exist", log.Uint64("now.instanceid", i.acceptor.GetInstanceID()))
		i.lastChecksum = 0
		return nil
	}

	i.lastChecksum = state.Checksum

	log.Info("ok", log.Uint32("last checksum", i.lastChecksum))
	return nil
}

func (i *Instance) GetNowInstanceID() uint64 {
	return i.acceptor.GetInstanceID()
}

func (i *Instance) GetMinChosenInstanceID() uint64 {
	return i.checkpointMgr.GetMaxChosenInstanceID()
}

func (i *Instance) GetLastChecksum() uint32 {
	return i.lastChecksum
}

func (i *Instance) GetInstanceValue(instanceID uint64) (value string, smID int, err error) {
	if instanceID >= i.acceptor.GetInstanceID() {
		return "", 0, ErrGetInstanceValueNotChosenYet
	}

	state, err := i.paxosLog.ReadState(i.config.GetMyGroupIdx(), instanceID)
	if err != nil && err != ErrNotExist {
		return "", 0, err
	}

	if err == ErrNotExist {
		return "", 0, ErrGetInstanceValueNotExist
	}

	smID = int(binary.BigEndian.Uint64(state.AcceptedValue[:sizeOfInt]))
	value = string(state.AcceptedValue[sizeOfInt:])
	return value, smID, nil
}

func (i *Instance) GetCommitter() *Committer {
	return i.committer
}

func (i *Instance) GetCheckpointCleaner() *Cleaner {
	return i.checkpointMgr.GetCleaner()
}

func (i *Instance) GetCheckpointReplayer() *Replayer {
	return i.checkpointMgr.GetReplayer()
}

func (i *Instance) CheckNewValue() {
	if !i.commitCtx.IsNewCommit() {
		return
	}
	if !i.learner.IsLatest() {
		return
	}
	if i.config.isFollower {
		log.Error("i'm follower,skip this new value")
		i.commitCtx.SetResultOnlyRet(ErrTryCommitFollowerCannotCommit)
		return
	}
	if !i.config.Check() {
		log.Error("i'm not in membership, skip this new value")
		i.commitCtx.SetResultOnlyRet(ErrTryCommitNotInMembership)
		return
	}
	if len(i.commitCtx.GetCommitValue()) > MaxValueSize() {
		log.Error("value too large, skip this new value", log.Int("value_size", len(i.commitCtx.GetCommitValue())))
		i.commitCtx.SetResultOnlyRet(ErrTryCommitValueSizeTooLarge)
		return
	}

	i.commitCtx.StartCommit(i.proposer.GetInstanceID())

	if i.commitCtx.GetTimeoutMs() != -1 {
		i.commitTimerID = i.loop.AddTimer(i.commitCtx.GetTimeoutMs(), TimerInstanceCommitTimeout)
	}

	if i.config.GetIsUseMembership() &&
		(i.proposer.GetInstanceID() == 0 || i.config.GetGid() == 0) {
		//Init system variables.
		log.Info("need to init system variables", log.Uint64("now.instanceid", i.proposer.GetInstanceID()),
			log.Uint64("now.gid", i.config.GetGid()))

		gid := GenGid(i.config.GetMyNodeID())
		initSVOpValue, err := i.config.GetSystemVSM().CreateGidOPValue(gid)
		if err != nil {
			panic(err)
		}

		i.smFac.PackPaxosValue(initSVOpValue, i.config.GetSystemVSM().SMID())
		err = i.proposer.NewValue(initSVOpValue)
		if err != nil {
			log.Error("proposer.new_value fail", log.Err(err))
		}
	} else {
		if i.options.OpenChangeValueBeforePropose {
			i.smFac.BeforePropose(i.config.GetMyGroupIdx(), i.commitCtx.GetCommitValue())
		}
		err := i.proposer.NewValue(i.commitCtx.GetCommitValue())
		if err != nil {
			log.Error("proposer.new_value fail", log.Err(err))
		}
	}

}

func (i *Instance) OnNewValueCommitTimeout() {
	i.proposer.ExitPrepare()
	i.proposer.ExitAccept()

	i.commitCtx.SetResult(ErrTryCommitTimeout, i.proposer.GetInstanceID(), "")
}

func (i *Instance) OnReceiveMessage(message []byte) error {
	err := i.loop.AddMessage(string(message))
	if err != nil {
		log.Error("loop.addmessage fail", log.Err(err))
		return err
	}
	return nil
}

func (i *Instance) OnReceive(buffer string) {
	if len(buffer) <= 6 {
		log.Error("buffer size too small", log.Int("buffer.size", len(buffer)))
		return
	}
	header, bodyStartPos, bodyLen, err := UnpackBaseMsg(buffer)
	if err != nil {
		return
	}

	cmd := header.CmdId
	if cmd == MsgCmdPaxosMsg {
		if i.checkpointMgr.InAskForCheckpointMode() {
			log.Info("in ask for checkpoint mode, ignored paxosmsg")
			return
		}

		paxosMsg := &PaxosMsg{}
		err = proto.Unmarshal([]byte(buffer[bodyStartPos:bodyStartPos+bodyLen]), paxosMsg)
		if err != nil {
			log.Error("paxos msg unmarshal fail, skip this msg", log.Err(err))
			return
		}

		if !i.receiveMsgHeaderCheck(header, NodeID(paxosMsg.NodeID)) {
			return
		}

		err = i.OnReceivePaxosMsg(paxosMsg, false)
		if err != nil {
			log.Error("on receive paxos msg fail", log.Err(err))
		}
	} else if cmd == MsgCmdCheckpointMsg {
		checkpointMsg := &CheckpointMsg{}
		err = proto.Unmarshal([]byte(buffer[bodyStartPos:bodyStartPos+bodyLen]), checkpointMsg)
		if err != nil {
			log.Error("checkpoint msg unmarshal fail, skip this msg", log.Err(err))
			return
		}
		if !i.receiveMsgHeaderCheck(header, NodeID(checkpointMsg.NodeID)) {
			return
		}
		i.OnReceiveCheckpointMsg(checkpointMsg)
	}
}

func (i *Instance) OnReceiveCheckpointMsg(msg *CheckpointMsg) {
	log.Info("on receive checkpoint message",
		log.Uint64("now.instanceid", i.acceptor.GetInstanceID()),
		log.Int32("msgType", msg.MsgType),
		log.Uint64("msg.from_nodeid", msg.NodeID),
		log.Uint64("my.nodeid", uint64(i.config.GetMyNodeID())),
		log.Int32("flag", msg.Flag),
		log.Uint64("uuid", msg.UUID),
		log.Uint64("sequence", msg.Sequence),
		log.Uint32("checksum", msg.Checksum),
		log.Uint64("offset", msg.Offset),
		log.Int("buffsize", len(msg.Buffer)),
		log.String("filepath", msg.FilePath))

	if msg.MsgType == CheckpointMsgTypeSendFile {
		if !i.checkpointMgr.InAskForCheckpointMode() {
			log.Info("not in ask for checkpoint mode, ignored checkpoint msg")
			return
		}
		i.learner.OnSendCheckpoint(msg)
	} else if msg.MsgType == CheckpointMsgTypeSendFileAck {
		i.learner.OnSendCheckpointAck(msg)
	}
}

func (i *Instance) OnReceivePaxosMsg(msg *PaxosMsg, isRetry bool) error {
	log.Info("on receive paxos message",
		log.Uint64("now.instanceid", i.acceptor.GetInstanceID()),
		log.Uint64("msg.instanceid", msg.InstanceID),
		log.Int32("msgType", msg.MsgType),
		log.Uint64("msg.from_nodeid", msg.NodeID),
		log.Uint64("my.nodeid", uint64(i.config.GetMyNodeID())),
		log.Uint64("seen.latest_instanceid", i.learner.GetSeenLatestInstanceID()))

	if msg.MsgType == MsgTypePaxosPrepareReply ||
		msg.MsgType == MsgTypePaxosAcceptReply ||
		msg.MsgType == MsgTypePaxosProposalSendNewValue {

		if !i.config.IsValidNodeID(NodeID(msg.NodeID)) {
			log.Error("acceptor reply type msg, from nodeid not in my membership, skip this message")
			return nil
		}

		return i.ReceiveMsgForProposer(msg)

	} else if msg.MsgType == MsgTypePaxosPrepare ||
		msg.MsgType == MsgTypePaxosAccept {

		//if my gid is zero, then this is a unknown node.
		if i.config.GetGid() == 0 {
			i.config.AddTmpNodeOnlyForLearn(NodeID(msg.NodeID))
		}

		if !i.config.IsValidNodeID(NodeID(msg.NodeID)) {
			log.Error("prepare/accept type msg, from nodeid not in my membership(or i'm null membership), "+
				"skip this message and add node to temp node", log.Uint64("my.gid", i.config.GetGid()))

			i.config.AddTmpNodeOnlyForLearn(NodeID(msg.NodeID))
			return nil
		}

		i.checksum(msg)
		return i.ReceiveMsgForAcceptor(msg, isRetry)

	} else if msg.MsgType == MsgTypePaxosLearnerAskForLearn ||
		msg.MsgType == MsgTypePaxosLearnerSendLearnValue ||
		msg.MsgType == MsgTypePaxosLearnerProposerSendSuccess ||
		msg.MsgType == MsgTypePaxosLearnerComfirmAskForLearn ||
		msg.MsgType == MsgTypePaxosLearnerSendNowInstanceID ||
		msg.MsgType == MsgTypePaxosLearnerSendLearnValueAck ||
		msg.MsgType == MsgTypePaxosLearnerAskForCheckpoint {

		i.checksum(msg)
		return i.ReceiveMsgForLearner(msg)
	} else {
		log.Error("invalid msgType", log.Int32("msg.type", msg.MsgType))
	}

	return nil
}

func (i *Instance) ReceiveMsgForProposer(msg *PaxosMsg) error {
	if i.config.IsIMFollower() {
		log.Error("i'm follower, skip this message")
		return nil
	}

	if msg.InstanceID != i.proposer.GetInstanceID() {
		//Expired reply msg on last instance.
		//If the response of a node is always slower than the majority node,
		//then the message of the node is always ignored even if it is a reject reply.
		//In this case, if we do not deal with these reject reply, the node that
		//gave reject reply will always give reject reply.
		//This causes the node to remain in catch-up state.
		//
		//To avoid this problem, we need to deal with the expired reply.
		if msg.MsgType == MsgTypePaxosPrepareReply {
			i.proposer.OnExpiredPrepareReply(msg)
		} else if msg.MsgType == MsgTypePaxosAcceptReply {
			i.proposer.OnExpiredAcceptReply(msg)
		}

		return nil
	}

	if msg.MsgType == MsgTypePaxosPrepareReply {
		i.proposer.OnPrepareReply(msg)
	} else if msg.MsgType == MsgTypePaxosAcceptReply {
		i.proposer.OnAcceptReply(msg)
	}

	return nil
}

func (i *Instance) ReceiveMsgForAcceptor(msg *PaxosMsg, isRetry bool) error {
	if i.config.IsIMFollower() {
		log.Error("i'm follower, skip this message")
		return nil
	}

	if msg.InstanceID != i.acceptor.GetInstanceID() {
		// 打点上报
	}

	if msg.InstanceID == i.acceptor.GetInstanceID()+1 {
		//skip success message
		newPaxosMsg := *msg
		newPaxosMsg.InstanceID = i.acceptor.GetInstanceID()
		newPaxosMsg.MsgType = MsgTypePaxosLearnerProposerSendSuccess
		_ = i.ReceiveMsgForLearner(&newPaxosMsg)
	}

	if msg.InstanceID == i.acceptor.GetInstanceID() {
		if msg.MsgType == MsgTypePaxosPrepare {
			return i.acceptor.OnPrepare(msg)
		} else if msg.MsgType == MsgTypePaxosAccept {
			_ = i.acceptor.OnAccept(msg)
		}

	} else if !isRetry && msg.InstanceID > i.acceptor.GetInstanceID() {
		//retry msg can't retry again.
		if msg.InstanceID >= i.learner.GetSeenLatestInstanceID() {
			if msg.InstanceID < i.acceptor.GetInstanceID()+RetryQueueMaxLen {
				//need retry msg precondition
				//1. prepare or accept msg
				//2. msg.instanceid > nowinstanceid.
				//    (if < nowinstanceid, this msg is expire)
				//3. msg.instanceid >= seen latestinstanceid.
				//    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
				//4. msg.instanceid close to nowinstanceid.
				_ = i.loop.AddRetryPaxosMsg(msg)

			} else {
				//retry msg not series, no use.
				i.loop.ClearRetryQueue()
			}
		}
	}

	return nil
}

func (i *Instance) ReceiveMsgForLearner(msg *PaxosMsg) error {
	switch msg.MsgType {
	case MsgTypePaxosLearnerAskForLearn:
		i.learner.OnAskForLearn(msg)
	case MsgTypePaxosLearnerSendLearnValue:
		i.learner.OnSendLearnValue(msg)
	case MsgTypePaxosLearnerProposerSendSuccess:
		i.learner.OnProposerSendSuccess(msg)
	case MsgTypePaxosLearnerSendNowInstanceID:
		i.learner.OnSendNowInstanceID(msg)
	case MsgTypePaxosLearnerComfirmAskForLearn:
		i.learner.OnConfirmAskForLearn(msg)
	case MsgTypePaxosLearnerSendLearnValueAck:
		i.learner.OnSendLearnValueAck(msg)
	case MsgTypePaxosLearnerAskForCheckpoint:
		i.learner.OnAskForCheckpoint(msg)
	}

	if i.learner.IsLearned() {
		smCtx, isMyCommit := i.commitCtx.IsMyCommit(i.learner.GetInstanceID(), i.learner.GetLearnValue())
		if !isMyCommit {
			log.Error("this value is not my commit")
		} else {
			log.Info("my commit ok")
		}

		if !i.SMExecute(i.learner.GetInstanceID(), i.learner.GetLearnValue(), isMyCommit, smCtx) {
			log.Error("SMExecute fail, instanceid not increase instanceid",
				log.Uint64("instanceid", i.learner.GetInstanceID()))
			i.commitCtx.SetResult(ErrTryCommitExecuteFail, i.learner.GetInstanceID(), i.learner.GetLearnValue())
			i.proposer.CancelSkipPrepare()
			return ErrTryCommitExecuteFail
		}
		{
			//this paxos instance end, tell proposal done
			i.commitCtx.SetResult(nil, i.learner.GetInstanceID(), i.learner.GetLearnValue())
			if i.commitTimerID > 0 {
				i.loop.RemoveTimer(i.commitTimerID)
			}
		}

		log.Info("[Learned] New paxos starting",
			log.Uint64("now.proposer.instanceid", i.proposer.GetInstanceID()),
			log.Uint64("now.acceptor.instanceid", i.acceptor.GetInstanceID()),
			log.Uint64("now.learner.instanceid", i.learner.GetInstanceID()))
		log.Info("[Learned] Checksum change",
			log.Uint32("last_checksum", i.lastChecksum),
			log.Uint32("new_checksum", i.learner.GetNewChecksum()))

		i.lastChecksum = i.learner.GetNewChecksum()
		i.newInstance()

		log.Info("[Learned] New paxos instance has started",
			log.Uint64("now.proposer.instanceid", i.proposer.GetInstanceID()),
			log.Uint64("now.acceptor.instanceid", i.acceptor.GetInstanceID()),
			log.Uint64("now.learner.instanceid", i.learner.GetInstanceID()))

		i.checkpointMgr.SetMaxChosenInstanceID(i.acceptor.GetInstanceID())
	}

	return nil
}

func (i *Instance) OnTimeout(timerID uint32, timerType int) {
	switch timerType {
	case TimerProposerPrepareTimeout:
		i.proposer.OnPrepareTimeout()
	case TimerProposerAcceptTimeout:
		i.proposer.OnAcceptTimeout()
	case TimerLearnerAskForLearnNoop:
		i.learner.AskForLearnNoop(false)
	case TimerInstanceCommitTimeout:
		i.OnNewValueCommitTimeout()
	default:
		log.Error("unknown timer type", log.Int("timerType", timerType),
			log.Uint32("timerID", timerID))
	}
}

func (i *Instance) AddStateMachine(sm StateMachine) {
	i.smFac.AddSM(sm)
}

func (i *Instance) SMExecute(instanceID uint64, value string, isMyCommit bool, smCtx *SMCtx) bool {
	return i.smFac.Execute(i.config.GetMyGroupIdx(), instanceID, value, smCtx)
}

func (i *Instance) checksum(msg *PaxosMsg) {
	if msg.LastChecksum == 0 {
		return
	}

	if msg.InstanceID != i.acceptor.GetInstanceID() {
		return
	}

	if i.acceptor.GetInstanceID() > 0 && i.GetLastChecksum() == 0 {
		log.Error("have no last checksum", log.Uint32("other.last_checksum", msg.LastChecksum))
		i.lastChecksum = msg.LastChecksum
		return
	}

	if msg.LastChecksum != i.GetLastChecksum() {
		log.Error("checksum fail", log.Uint32("my.last_checksum", i.GetLastChecksum()),
			log.Uint32("other.last_checksum", msg.LastChecksum))
		panic("checksum fail")
	}
}

func (i *Instance) playLog(beginInstanceID, endInstanceID uint64) error {
	if beginInstanceID < i.checkpointMgr.GetMinChosenInstanceID() {
		log.Error("begin instanceid small than min chosen instanceid",
			log.Uint64("begin_instanceid", beginInstanceID),
			log.Uint64("min_chosen_instanceid", i.checkpointMgr.GetMinChosenInstanceID()))
		return errors.New("invalid begin instanceid")
	}

	myGroupIdx := i.config.GetMyGroupIdx()
	for instanceID := beginInstanceID; instanceID < endInstanceID; instanceID++ {
		state, err := i.paxosLog.ReadState(myGroupIdx, instanceID)
		if err != nil {
			log.Error("log read fail", log.Uint64("instanceid", instanceID), log.Err(err))
			return err
		}

		ok := i.smFac.Execute(myGroupIdx, instanceID, string(state.AcceptedValue), nil)
		if !ok {
			log.Error("sm execute fail", log.Uint64("instanceid", instanceID))
			return errors.New("sm execute fail")
		}
	}
	return nil
}

func (i *Instance) receiveMsgHeaderCheck(header *Header, fromNodeID NodeID) bool {
	if i.config.GetGid() == 0 || header.Gid == 0 {
		return true
	}

	if i.config.GetGid() != header.Gid {
		log.Error("header check fail", log.Uint64("header,gid", header.Gid),
			log.Uint64("config.gid", i.config.GetGid()),
			log.Uint64("msg.from_nodeid", uint64(fromNodeID)))
		return false
	}
	return true
}

func (i *Instance) isCheckpointInstanceIDCorrect(checkpointInstanceID, logMaxInstanceID uint64) error {
	if checkpointInstanceID <= logMaxInstanceID {
		return nil
	}
	//checkpoint_instanceid larger than log_maxinstanceid+1 will appear in the following situations
	//1. Pull checkpoint from other node automatically and restart. (normal case)
	//2. Paxos log was manually all deleted. (may be normal case)
	//3. Paxos log is lost because Options::bSync set as false. (bad case)
	//4. Checkpoint data corruption results an error checkpoint_instanceid. (bad case)
	//5. Checkpoint data copy from other node manually. (bad case)
	//In these bad cases, paxos log between [log_maxinstanceid, checkpoint_instanceid) will not exist
	//and checkpoint data maybe wrong, we can't ensure consistency in this case.
	if logMaxInstanceID == 0 {
		minChosenInstanceID := i.checkpointMgr.GetMinChosenInstanceID()
		if minChosenInstanceID != checkpointInstanceID {
			err := i.checkpointMgr.SetMinChosenInstanceID(checkpointInstanceID)
			if err != nil {
				log.Error("SetMinChosenInstanceID fail", log.Err(err),
					log.Uint64("now_minchosen", i.checkpointMgr.GetMinChosenInstanceID()),
					log.Uint64("log.max", logMaxInstanceID),
					log.Uint64("checkpoint", checkpointInstanceID))
				return err
			}
			log.Info("Fix minchonse instanceid ok",
				log.Uint64("old_minchosen", minChosenInstanceID),
				log.Uint64("now_minchosen", i.checkpointMgr.GetMinChosenInstanceID()),
				log.Uint64("log.max", logMaxInstanceID),
				log.Uint64("checkpoint", checkpointInstanceID))
		}

		return nil
	}
	//other case.
	log.Error("checkpoint instanceid larger than log max instanceid. "+
		"Please ensure that your checkpoint data is correct. "+
		"If you ensure that, just delete all paxos log data and restart.",
		log.Uint64("checkpoint.instanceid", checkpointInstanceID),
		log.Uint64("log.max_instanceid", logMaxInstanceID))
	return errors.New("checkpoint invalid")
}

func (i *Instance) newInstance() {
	i.acceptor.NewInstance()
	i.learner.NewInstance()
	i.proposer.NewInstance()
}
