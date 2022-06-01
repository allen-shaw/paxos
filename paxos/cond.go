package paxos

import (
	"sync"
	"time"
)

type Cond struct {
	cond *sync.Cond
}

func NewCond(l sync.Locker) *Cond {
	c := &Cond{}
	c.cond = sync.NewCond(l)
	return c
}

func (c *Cond) Broadcast() {
	c.cond.Broadcast()
}

func (c *Cond) Signal() {
	c.cond.Signal()
}

func (c *Cond) Wait() {
	c.cond.Wait()
}

func (c *Cond) WaitFor(d time.Duration) bool {
	timeout := false
	timer := time.NewTimer(d)
	defer timer.Stop()
	go func() {
		_ = <-timer.C
		c.cond.Broadcast()
		timeout = true
	}()

	c.Wait()
	return timeout
}
