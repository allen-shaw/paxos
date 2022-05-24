package paxos

import "sync"

type SerialLock struct {
	mutex sync.Mutex
	cond  *sync.Cond
}

func NewLock() *SerialLock {
	l := &SerialLock{}
	l.cond = sync.NewCond(&l.mutex)
	return l
}

func (l *SerialLock) Lock() {
	l.mutex.Lock()
}
func (l *SerialLock) UnLock() {
	l.mutex.Unlock()
}

func (l *SerialLock) Wait() {
	l.cond.Wait()
}

func (l *SerialLock) Interrupt() {
	l.cond.Signal()
}
