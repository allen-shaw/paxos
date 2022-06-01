package paxos

import (
	"encoding/binary"
	"github.com/AllenShaw19/paxos/log"
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

}

func (i *Instance) OnNewValueCommitTimeout() {

}

func (i *Instance) OnReceiveMessage(message []byte) error {

}

func (i *Instance) OnReceive(buffer string) {

}

func (i *Instance) OnReceiveCheckpointMsg(mgr *CheckpointMgr) {

}

func (i *Instance) OnReceivePaxosMsg(msg *PaxosMsg, isRetry bool) error {

}

func (i *Instance) ReceiveMsgForProposer(msg *PaxosMsg) error {

}

func (i *Instance) ReceiveMsgForAcceptor(msg *PaxosMsg, isRetry bool) error {

}

func (i *Instance) ReceiveMsgForLearner(msg *PaxosMsg) error {

}

func (i *Instance) OnTimeout(timerID uint32, t int) {

}

func (i *Instance) AddStateMachine(sm StateMachine) {

}

func (i *Instance) SMExecute(instanceID uint64, value string, isMyCommit bool, smCtx *SMCtx) bool {

}

func (i *Instance) checksum(msg *PaxosMsg) {

}

func (i *Instance) playLog(beginInstanceID, endInstanceID uint64) error {

}

func (i *Instance) receiveMsgHeaderCheck(header *Header, fromNodeID NodeID) bool {

}

func (i *Instance) isCheckpointInstanceIDCorrect(cpInstanceID, maxInstanceID uint64) error {

}

func (i *Instance) newInstance() {

}
