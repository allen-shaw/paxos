package paxos

type Client interface {
	SendMessage(groupIdx int, ip string, port int, message string) error
}

type Server interface {
	Run() error
	Stop() error
	OnReceiveMessage(message []byte) error
	Init(ip string, port int) error
}

type NetWork interface {
	Client
	Server
	SetNode(node Node)
}

type netWork struct {
	node Node
}

func NewNetWork() *netWork {
	return &netWork{}
}

func (n *netWork) Init(ip string, port int) error {

	return nil
}

func (n *netWork) Run() error {

	return nil
}

func (n *netWork) Stop() error {

	return nil
}

func (n *netWork) SendMessage(groupIdx int, ip string, port int, message string) error {

	return nil
}

func (n *netWork) OnReceiveMessage(message []byte) error {

	return nil
}

func (n *netWork) SetNode(node Node) {
	n.node = node
}
