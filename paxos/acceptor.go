package paxos

type AcceptorState struct {
}

type Acceptor struct {
	*base
	state *AcceptorState
}

func NewAcceptor(config *Config,
	msgTransport MsgTransport,
	instance *Instance,
	logStorage LogStorage) *Acceptor {

}

func (a *Acceptor) Init() error {

}
