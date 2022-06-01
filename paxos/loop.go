package paxos

import "github.com/AllenShaw19/paxos/log"

type Loop struct {
	isEnd   bool
	isStart bool

	timer    Timer
	timerIDs map[uint32]bool

	messageQueue chan string
	retryQueue   []*PaxosMsg

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
	go l.Run()
}

func (l *Loop) Run() {
	l.isEnd = false
	l.isStart = true
	for {
		nextTimeout := 1000
		l.DealWithTimeout(nextTimeout)

		l.OneLoop(nextTimeout)
		if l.isEnd {
			log.Info("Loop [END]")
			break
		}
	}
}

func (l *Loop) Stop() {
	l.isEnd = true
	if l.isStart {
		// join?
	}
}

func (l *Loop) OneLoop(timeoutMs int) {

}

func (l *Loop) DealWithRetry() {
	if len(l.retryQueue) == 0 {
		return
	}

	haveRetryOne := false
	for {
		select {}
	}
}

func (l *Loop) ClearRetryQueue() {

}

func (l *Loop) AddMessage(message string) error {
	l.messageQueue <- message
	return nil
}

func (l *Loop) AddRetryPaxosMsg(paxosMsg *PaxosMsg) error {
	l.retryQueue <- paxosMsg
}

func (l *Loop) AddNotify() {
	l.messageQueue <- ""
}

func (l *Loop) AddTimer(timeoutMs int, timerType TimerType) (uint32, bool) {

}

func (l *Loop) RemoveTimer(timerID uint32) bool {

}

func (l *Loop) DealWithTimeout(nextTimeout int) int {
	hasTimeout := true
	for hasTimeout {
		var (
			timerID   uint32
			timerType int
		)
		timerID, timerType, hasTimeout = l.timer.PopTimeout()
		if hasTimeout {
			l.DealWithTimeoutOne(timerID, timerType)
			nextTimeout = l.timer.GetNextTimeout()
			if nextTimeout != 0 {
				break
			}
		}
	}
}

func (l *Loop) DealWithTimeoutOne(timerID uint32, timerType int) {
	if _, ok := l.timerIDs[timerID]; !ok {
		log.Error("timeout already remove.", log.Uint32("timerid", timerID), log.Int("type", timerType))
		return
	}
	delete(l.timerIDs, timerID)
	l.instance.OnTimeout(timerID, timerType)
}
