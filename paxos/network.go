package paxos

import (
	"fmt"
	"github.com/AllenShaw19/paxos/plugin/log"
	"net"
)

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

/////////////////////////////////////////

type netWork struct {
	addr  string
	conns map[string]net.Conn
	node  Node
	doneC chan struct{}
}

func NewNetWork() *netWork {
	return &netWork{
		conns: make(map[string]net.Conn),
		doneC: make(chan struct{}),
	}
}

func (n *netWork) Init(ip string, port int) error {
	n.addr = fmt.Sprintf("%s:%d", ip, port)
	return nil
}

func (n *netWork) Run() error {
	listener, err := net.Listen("tcp", "localhost:50000")
	if err != nil {
		return err
	}
	for {
		select {
		case <-n.doneC:
			return nil
		default:
		}
		conn, err := listener.Accept()
		if err != nil {
			log.Error("listener accept fail", log.Err(err))
			continue
		}
		go n.handle(conn)
	}
}

func (n *netWork) Stop() error {
	close(n.doneC)
	return nil
}

func (n *netWork) SendMessage(groupIdx int, ip string, port int, message string) error {
	addr := fmt.Sprintf("%s:%d", ip, port)
	conn, ok := n.conns[addr]
	if !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		n.conns[addr] = conn
	}
	_, err := conn.Write([]byte(message))
	if err != nil {
		return err
	}
	return nil
}

func (n *netWork) OnReceiveMessage(message []byte) error {
	err := n.node.OnReceiveMessage(string(message))
	if err != nil {
		return err
	}
	return nil
}

func (n *netWork) SetNode(node Node) {
	n.node = node
}

func (n *netWork) handle(conn net.Conn) {
	msg := make([]byte, 0)
	for {
		// TODO: 未实现
		_, err := conn.Read(msg)
		if err != nil {
			log.Error("reading fail", log.Err(err))
			return //终止程序
		}
	}
	n.node.OnReceiveMessage(string(msg))
}
