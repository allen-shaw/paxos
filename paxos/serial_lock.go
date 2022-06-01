package paxos

import "sync"

type SerialLock struct {
	mutex sync.Mutex
	cond  *sync.Cond
}

func NewSerialLock() *SerialLock {
	l := &SerialLock{}
	l.cond = sync.NewCond(&l.mutex)
	return l
}

func (l *SerialLock) Lock() {
	l.mutex.Lock()
}
func (l *SerialLock) Unlock() {
	l.mutex.Unlock()
}

func (l *SerialLock) Wait() {
	l.cond.Wait()
}

func (l *SerialLock) WaitTime(timeMs int) bool {
	// TODO:
	l.cond.Wait()
	return true
}

func (l *SerialLock) Interrupt() {
	l.cond.Signal()
}
