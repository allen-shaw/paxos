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

func (l *Loop) Start() {

}

func (l *Loop) RemoveTimer(timerID uint32) {

}

func (l *Loop) AddTimer(timeoutMs int, timerType TimerType) uint32 {

}

func (l *Loop) Stop() {

}
