package paxos

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
	i.proposer = NewProposer(config, msgTransport, i, logStorage, i.loop)
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
	i.acceptor.Init()
}

func (i *Instance) Start() {

}

func (i *Instance) Stop() {

}

func (i *Instance) InitLastCheckSum() {

}

func (i *Instance) GetNowInstanceID() uint64 {

}

func (i *Instance) GetMinChosenInstanceID() uint64 {

}

func (i *Instance) GetLastChecksum() uint32 {

}

func (i *Instance) GetInstanceValue(instanceID uint64) (value string, smID int) {

}

func (i *Instance) GetCommitter() *Committer {

}

func (i *Instance) GetCheckpointCleaner() *Cleaner {

}

func (i *Instance) GetCheckpointReplayer() *Replayer {

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
