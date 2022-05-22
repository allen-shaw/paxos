package paxos

type Learner struct {
}

func NewLearner(config *Config,
	msgTransport MsgTransport,
	instance *Instance,
	acceptor *Acceptor,
	logStorage LogStorage,
	loop *Loop,
	checkpointMgr *CheckpointMgr,
	smFac *SMFac) *Learner {

}

func (l *Learner) ProposerSendSuccess(learnInstanceID, proposalID uint64) {

}
