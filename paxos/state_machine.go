package paxos

import (
	"math"
)

type SMCtx struct {
	SMID int
	Ctx  interface{} // TODO: 可能需要定义一个interface类型
}

func NewSMCtx(smID int, ctx interface{}) *SMCtx {
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
	//This 'success' means this executes don't need to retry.
	//Sometimes you will have some logical failure in your execute logic,
	//and this failure will definite occur on all node, that means this failure is acceptable,
	//for this case, return true is the best choice.
	//Some system failure will let different node's execute result inconsistent,
	//for this case, you must return false to retry this executes to avoid this system failure.
	Execute(groupIdx int, instanceID uint64, paxosValue string, ctx *SMCtx) bool

	ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue string) bool

	// GetCheckpointInstanceID Only need to implement this function while you have checkpoint.
	// Return your checkpoint's max executed instance id.
	// Notice Paxos will call this function very frequently.
	GetCheckpointInstanceID(groupIdx int) uint64

	// LockCheckpointState After called this function, the FileList that
	// GetCheckpointState return's, can't be deleted, moved and modified.
	LockCheckpointState() error

	// GetCheckpointState dirPath is checkpoint data root dir path.
	// files is the relative path of the dirPath.
	GetCheckpointState(groupIdx int) (dirPath string, files []string, err error)

	UnLockCheckpointState()

	// LoadCheckpointState Checkpoint file was on dir(checkpointTmpFileDirPath).
	// files is all the file in dir(checkpointTmpFileDirPath).
	// files filepath is absolute path.
	// After called this function, paxoslib will kill the processor.
	// State machine need to understand this when restarted.
	LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, files []string, checkpointInstanceID uint64) error

	// BeforePropose You can modify your request at this moment.
	// At this moment, the state machine data will be up-to-date.
	// If request is batch, propose requests for multiple identical state machines will only call this function once.
	// Ensure that the execute function correctly recognizes the modified request.
	// Since this function is not always called, the execute function must handle the unmodified request correctly.
	BeforePropose(groupIdx int, value string) (string, error)

	// NeedCallBeforePropose Because function BeforePropose much waste cpu,
	// Only NeedCallBeforePropose return true then weill call function BeforePropose.
	// You can use this function to control call frequency.
	// Default is false.
	NeedCallBeforePropose() bool
}
