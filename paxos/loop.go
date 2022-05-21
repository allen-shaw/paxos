package paxos

import "time"

type Loop struct {
	isEnd   bool
	isStart bool

	timer time.Timer

	config   *Config
	instance *Instance
}

func NewLoop(config *Config, instance *Instance) *Loop {
	l := &Loop{}
	l.isEnd = false
	l.isStart = false
	l.config = config
	l.instance = instance
	return l
}
