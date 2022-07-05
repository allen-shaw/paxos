package paxos

import (
	"errors"
	"github.com/AllenShaw19/paxos/plugin/log"
)

const RetryQueueMaxLen = 300

type Loop struct {
	isEnd   bool
	isStart bool

	timer    Timer
	timerIDs map[uint32]bool

	messageQueue *Queue[*string]
	retryQueue   queue[*PaxosMsg]

	config   *Config
	instance *Instance
}

func NewLoop(config *Config, instance *Instance) *Loop {
	l := &Loop{}
	l.isEnd = false
	l.isStart = false

	l.messageQueue = NewQueue[*string]()
	l.retryQueue = make([]*PaxosMsg, 0)

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
	l.messageQueue.Lock()
	message, ok := l.messageQueue.Peek(timeoutMs)
	if !ok {
		l.messageQueue.Unlock()
	} else {
		l.messageQueue.Pop()
		l.messageQueue.Unlock()

		if message != nil && len(*message) > 0 {
			l.instance.OnReceive(*message)
		}
	}

	l.DealWithRetry()
	//must put on here
	//because addtimer on this funciton
	l.instance.CheckNewValue()
}

func (l *Loop) DealWithRetry() {
	if len(l.retryQueue) == 0 {
		return
	}

	haveRetryOne := false
	for !l.retryQueue.Empty() {
		paxosMsg := l.retryQueue.Front()
		if paxosMsg.InstanceID > l.instance.GetNowInstanceID()+1 {
			break
		} else if paxosMsg.InstanceID == l.instance.GetNowInstanceID()+1 {
			//only after retry i == now_i, than we can retry i + 1.
			if haveRetryOne {
				log.Info("retry msg (i+1).", log.Uint64("instanceid", paxosMsg.InstanceID))
				err := l.instance.OnReceivePaxosMsg(paxosMsg, true)
				if err != nil {
					log.Error("instance on receive paxos msg fail", log.Err(err), log.Uint64("instanceid", paxosMsg.InstanceID))
				}
			} else {
				break
			}
		} else if paxosMsg.InstanceID == l.instance.GetNowInstanceID() {
			log.Info("retry msg", log.Uint64("instanceid", paxosMsg.InstanceID))
			err := l.instance.OnReceivePaxosMsg(paxosMsg, false)
			if err != nil {
				log.Error("instance on receive paxos msg fail", log.Err(err), log.Uint64("instanceid", paxosMsg.InstanceID))
			}
		}
		l.retryQueue.PopFront()
	}
}

func (l *Loop) ClearRetryQueue() {
	l.retryQueue = make([]*PaxosMsg, 0)
}

func (l *Loop) AddMessage(message string) error {
	l.messageQueue.Lock()
	defer l.messageQueue.Unlock()

	if l.messageQueue.Size() > QueueMaxlength() {
		log.Error("queue full, skip msg")
		return errors.New("queue full")
	}

	l.messageQueue.Add(&message)
	return nil
}

func (l *Loop) AddRetryPaxosMsg(paxosMsg *PaxosMsg) error {
	if len(l.retryQueue) > RetryQueueMaxLen {
		l.retryQueue = l.retryQueue[1:]
	}
	l.retryQueue = append(l.retryQueue, paxosMsg)
	return nil
}

func (l *Loop) AddNotify() {
	l.messageQueue.Lock()
	defer l.messageQueue.Unlock()
	l.messageQueue.Add(nil)
}

func (l *Loop) AddTimer(timeoutMs int, timerType TimerType) (timerID uint32) {
	if timeoutMs == -1 {
		return 0
	}
	absTime := GetCurrentTimeMs() + uint64(timeoutMs)
	timerID = l.timer.AddTimerWithType(absTime, int(timerType))

	l.timerIDs[timerID] = true
	return timerID
}

func (l *Loop) RemoveTimer(timerID uint32) {
	delete(l.timerIDs, timerID)
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
	return nextTimeout
}

func (l *Loop) DealWithTimeoutOne(timerID uint32, timerType int) {
	if _, ok := l.timerIDs[timerID]; !ok {
		log.Error("timeout already remove.", log.Uint32("timerid", timerID), log.Int("type", timerType))
		return
	}
	delete(l.timerIDs, timerID)
	l.instance.OnTimeout(timerID, timerType)
}
