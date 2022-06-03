package paxos

import "sync"

type PendingProposal struct {
	Value *string
	SMCtx *SMCtx

	// return parameter
	InstanceID *uint64
	BatchIndex *uint32

	Notifier       *Notifier
	AbsEnqueueTime uint64
}

type ProposeBatch struct {
	myGroupIdx   int
	node         Node
	notifierPool *NotifierPool

	mutex sync.Mutex
	cond  *sync.Cond
	queue queue[PendingProposal]

	isEnd     bool
	isStarted bool

	batchCount       int
	batchDelayTimeMs int
	batchMaxSize     int
}
