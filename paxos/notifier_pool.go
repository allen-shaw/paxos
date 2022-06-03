package paxos

import "sync"

type Notifier struct {
	sendC chan error
	recvC chan error
}

func NewNotifier() *Notifier {
	return &Notifier{
		sendC: make(chan error),
		recvC: make(chan error),
	}
}

func (n *Notifier) Close() {
	if n.sendC != nil {
		close(n.sendC)
	}
	if n.recvC != nil {
		close(n.recvC)
	}
}

func (n *Notifier) SendNotify(ret error) {
	n.sendC <- ret
}

func (n *Notifier) WaitNotify() error {
	return <-n.recvC
}

////////////////////////

type NotifierPool struct {
	mutex sync.Mutex
	pool  map[uint64]*Notifier
}

func NewNotifierPool() *NotifierPool {
	return &NotifierPool{
		pool: make(map[uint64]*Notifier, 0),
	}
}

func (p *NotifierPool) Close() {
	for _, n := range p.pool {
		n.Close()
	}
	p.pool = make(map[uint64]*Notifier, 0)
}

func (p *NotifierPool) GetNotifier(id uint64) (*Notifier, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if notifier, ok := p.pool[id]; ok {
		return notifier, nil
	}

	notifier := NewNotifier()
	p.pool[id] = notifier
	return notifier, nil
}
