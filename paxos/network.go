package paxos

type Client interface {
	SendMessage(groupIdx int, ip string, port int, message string) error
}

type Server interface {
	Run() error
	Stop() error
	OnReceiveMessage(message []byte) error
}

type NetWork interface {
	Client
	Server
}
