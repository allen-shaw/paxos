package paxos

import (
	"github.com/AllenShaw19/paxos/plugin/log"
	"math"
)

type CommitCtx struct {
	config     *Config
	instanceID uint64

	commitRet   error
	isCommitEnd bool
	timeoutMs   int

	value *string
	smCtx *SMCtx
	lock  SerialLock
}

func NewCommitCtx(config *Config) *CommitCtx {
	ctx := &CommitCtx{config: config}
	ctx.NewCommit(nil, nil, 0)
	return ctx
}

func (c *CommitCtx) NewCommit(value *string, smCtx *SMCtx, timeoutMs int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.instanceID = math.MaxUint64
	c.commitRet = nil
	c.isCommitEnd = false
	c.timeoutMs = timeoutMs

	c.value = value
	c.smCtx = smCtx
	if value != nil {
		log.Info("ok", log.Int("value.size", len(*value)))
	}
}

func (c *CommitCtx) IsNewCommit() bool {
	return c.instanceID == math.MaxUint64 && c.value != nil
}

func (c *CommitCtx) GetCommitValue() string {
	return *c.value
}
func (c *CommitCtx) StartCommit(instanceID uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.instanceID = instanceID
}

func (c *CommitCtx) IsMyCommit(instanceID uint64, learnValue string) (*SMCtx, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	isMyCommit := false

	if c.isCommitEnd && c.instanceID == instanceID {
		isMyCommit = learnValue == *c.value
	}

	if isMyCommit {
		return c.smCtx, isMyCommit
	}
	return nil, isMyCommit
}

func (c *CommitCtx) SetResultOnlyRet(commitRet error) {
	c.SetResult(commitRet, math.MaxUint64, "")
}

func (c *CommitCtx) SetResult(commitRet error, instanceID uint64, learnValue string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.isCommitEnd || c.instanceID == instanceID {
		return
	}

	c.commitRet = commitRet
	if c.commitRet == nil {
		if *c.value != learnValue {
			c.commitRet = ErrTryCommitConflict
		}
	}

	c.isCommitEnd = true
	c.value = nil
}

func (c *CommitCtx) GetResult() (succInstanceID uint64, err error) {
	c.lock.Lock()
	for !c.isCommitEnd {
		c.lock.WaitTime(1000)
	}

	if c.commitRet == nil {
		succInstanceID = c.instanceID
		log.Info("commit success", log.Uint64("instance_id", succInstanceID))
	} else {
		log.Error("commit fail", log.Err(c.commitRet))
	}
	return succInstanceID, c.commitRet
}

func (c *CommitCtx) GetTimeoutMs() int {
	return c.timeoutMs
}
