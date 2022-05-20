package paxos

import (
	"context"
	"math"
)

type SMCtx struct {
	SMID int
	Ctx  context.Context
}

func NewSMCtx(smID int, ctx context.Context) *SMCtx {
	return &SMCtx{SMID: smID, Ctx: ctx}
}

type CheckpointFileInfo struct {
	FilePath string
	FileSize uint64
}

type CheckpointFileInfoList []CheckpointFileInfo

const NoCheckpoint = math.MaxUint64

type StateMachine interface {
	// SMID Different state machine return different SMID().
	SMID() int

	// Execute Return true means execute success.
	//This 'success' means this execute don't need to retry.
	//Sometimes you will have some logical failure in your execute logic,
	//and this failure will definite occur on all node, that means this failure is acceptable,
	//for this case, return true is the best choice.
	//Some system failure will let different node's execute result inconsistent,
	//for this case, you must return false to retry this execute to avoid this system failure.
	Execute(groupIdx int, instanceID uint64, paxosValue string, ctx *SMCtx) error
}
