package paxos

type Instance struct {
	config       *Config
	msgTransport MsgTransport
}
