package paxos

import "github.com/AllenShaw19/paxos/log"

type Committer struct {
	config    *Config
	commitCtx *CommitCtx
	loop      *Loop
	smFac     *SMFac

	waitLock  *WaitLock
	timeoutMs int

	lastLogTime uint64
}

func NewCommitter(config *Config,
	commitCtx *CommitCtx,
	loop *Loop,
	smFac *SMFac) *Committer {
	c := &Committer{
		config:      config,
		commitCtx:   commitCtx,
		loop:        loop,
		smFac:       smFac,
		waitLock:    NewWaitLock(),
		timeoutMs:   -1,
		lastLogTime: GetCurrentTimeMs(),
	}
	return c
}

func (c *Committer) NewValue(value string) (instanceID uint64, err error) {
	return c.NewValueGetID(value)
}

func (c *Committer) NewValueGetID(value string) (instanceID uint64, err error) {
	return c.NewValueGetIDWithSMCtx(value, nil)
}

func (c *Committer) NewValueGetIDWithSMCtx(value string, smCtx *SMCtx) (instanceID uint64, err error) {
	retryCount := 3

	for ; retryCount > 0; retryCount-- {
		instanceID, err = c.NewValueGetIDNoRetry(value, smCtx)
		if err != ErrTryCommitConflict {
			break
		}

		if smCtx != nil && smCtx.SMID == MasterVSMID {
			//master sm not retry
			break
		}
	}

	return instanceID, err
}

func (c *Committer) NewValueGetIDNoRetry(value string, smCtx *SMCtx) (uint64, error) {
	c.LogStatus()

	lockUseTimeMs, hasLock := c.waitLock.Lock(c.timeoutMs)
	if !hasLock {
		if lockUseTimeMs > 0 {
			// TODO 打点
			log.Error("try get lock, but timeout", log.Int("locktime ms", lockUseTimeMs))
			return 0, ErrTryCommitTimeout
		} else {
			log.Error("try get lock, but too many thread waiting, reject")
			return 0, ErrTryCommitTooManyThreadWaitingReject
		}
	}

	leftTimeoutMs := -1
	if c.timeoutMs > 0 {
		leftTimeoutMs = c.timeoutMs - lockUseTimeMs
		if leftTimeoutMs < 0 {
			leftTimeoutMs = 0
		}
		if leftTimeoutMs < 200 {
			log.Error("get lock ok, but lockusetime too long",
				log.Int("lockusetime", lockUseTimeMs),
				log.Int("lefttimeout", leftTimeoutMs))
			c.waitLock.Unlock()
			return 0, ErrTryCommitTimeout
		}
	}

	log.Info("get lock ok", log.Int("usetime", lockUseTimeMs))
	smID := 0
	if smCtx != nil {
		smID = smCtx.SMID
	}

	packedSMIDValue := c.smFac.PackPaxosValue(value, smID)
	c.commitCtx.NewCommit(&packedSMIDValue, smCtx, leftTimeoutMs)
	c.loop.AddNotify()

	instanceID, err := c.commitCtx.GetResult()
	return instanceID, err
}

func (c *Committer) SetTimeoutMs(timeoutMs int) {
	c.timeoutMs = timeoutMs
}

func (c *Committer) SetMaxHoldThreads(maxHoldThreads int) {
	c.waitLock.SetMaxWaitLockCount(maxHoldThreads)
}

func (c *Committer) SetProposeWaitTimeThresholdMS(waitTimeThresholdMs int) {
	c.waitLock.SetLockWaitTimeThresholdMs(waitTimeThresholdMs)
}

func (c *Committer) LogStatus() {
	nowTime := GetCurrentTimeMs()
	if nowTime > c.lastLogTime && nowTime-c.lastLogTime > 1000 {
		c.lastLogTime = nowTime
		log.Info("committer", log.Int("wait_threads", c.waitLock.GetNowHoldThreadCount()),
			log.Int("avg_thread_wait_ms", c.waitLock.GetNowAvgThreadWaitTime()),
			log.Int("reject_rate", c.waitLock.GetNowRejectRate()))
	}
}
