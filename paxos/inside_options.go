package paxos

import "math/rand"

var insideOptions *InsideOptions

type InsideOptions struct {
	isLargeBufferMode bool
	isFollower        bool
	groupCount        int
}

func NewInsideOptions() *InsideOptions {
	return &InsideOptions{
		isLargeBufferMode: false,
		isFollower:        false,
		groupCount:        1,
	}
}

func InsideOptionsInstance() *InsideOptions {
	if insideOptions == nil {
		insideOptions = NewInsideOptions()
	}
	return insideOptions
}

func (i *InsideOptions) SetAsLargeBufferMode() {
	i.isLargeBufferMode = true
}

func (i *InsideOptions) SetAsFollower() {
	i.isFollower = true
}

func (i *InsideOptions) SetGroupCount(groupCount int) {
	i.groupCount = groupCount
}

func (i *InsideOptions) GetMaxBufferSize() int {
	if i.isLargeBufferMode {
		return 52428800
	}
	return 10485760
}

func (i *InsideOptions) GetStartPrepareTimeoutMs() int {
	if i.isLargeBufferMode {
		return 15000
	}
	return 2000
}

func (i *InsideOptions) GetStartAcceptTimeoutMs() int {
	if i.isLargeBufferMode {
		return 15000
	}
	return 1000
}

func (i *InsideOptions) GetMaxPrepareTimeoutMs() int {
	if i.isLargeBufferMode {
		return 90000
	}
	return 8000
}

func (i *InsideOptions) GetMaxAcceptTimeoutMs() int {
	if i.isLargeBufferMode {
		return 90000
	}
	return 8000
}

func (i *InsideOptions) GetMaxIOLoopQueueLen() int {
	if i.isLargeBufferMode {
		return 1024/i.groupCount + 100
	}
	return 10240/i.groupCount + 1000
}

func (i *InsideOptions) GetMaxQueueLen() int {
	if i.isLargeBufferMode {
		return 1024
	}
	return 10240
}

func (i *InsideOptions) GetAskForLearnInterval() int {
	if !i.isFollower {
		if i.isLargeBufferMode {
			return 50000 + rand.Intn(10000)
		}
		return 2500 + rand.Intn(500)
	} else {
		if i.isLargeBufferMode {
			return 30000 + rand.Intn(15000)
		}
		return 2000 + rand.Intn(1000)
	}
}

func (i *InsideOptions) GetLearnerReceiverAckLead() int {
	if i.isLargeBufferMode {
		return 2
	}
	return 4
}

func (i *InsideOptions) GetLearnerSenderPrepareTimeoutMs() int {
	if i.isLargeBufferMode {
		return 6000
	}
	return 5000
}

func (i *InsideOptions) GetLearnerSenderAckTimeoutMs() int {
	if i.isLargeBufferMode {
		return 60000
	}
	return 5000
}

func (i *InsideOptions) GetLearnerSenderAckLead() int {
	if i.isLargeBufferMode {
		return 5
	}
	return 21
}

func (i *InsideOptions) GetTcpOutQueueDropTimeMs() int {
	if i.isLargeBufferMode {
		return 20000
	}
	return 5000
}

func (i *InsideOptions) GetLogFileMaxSize() int {
	if i.isLargeBufferMode {
		return 524288000
	}
	return 104857600
}

func (i *InsideOptions) GetTcpConnectionNonActiveTimeout() int {
	if i.isLargeBufferMode {
		return 600000
	}
	return 60000
}

func (i *InsideOptions) GetLearnerSenderSendQps() int {
	if i.isLargeBufferMode {
		return 10000 / i.groupCount
	}
	return 100000 / i.groupCount
}

func (i *InsideOptions) GetCleanerDeleteQps() int {
	if i.isLargeBufferMode {
		return 30000 / i.groupCount
	}
	return 300000 / i.groupCount
}

func LogFileMaxSize() int {
	return InsideOptionsInstance().GetLogFileMaxSize()
}
