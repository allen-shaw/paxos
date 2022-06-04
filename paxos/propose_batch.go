package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)

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
	batchID uint64

	myGroupIdx   int
	node         Node
	notifierPool *NotifierPool

	mutex sync.Mutex
	cond  *Cond
	queue queue[*PendingProposal]

	isEnd     bool
	isStarted bool

	batchCount       int
	batchDelayTimeMs int
	batchMaxSize     int
}

func NewProposeBatch(groupIdx int, node Node, notifierPool *NotifierPool) *ProposeBatch {
	b := &ProposeBatch{
		myGroupIdx:       groupIdx,
		node:             node,
		notifierPool:     notifierPool,
		isEnd:            false,
		isStarted:        false,
		batchCount:       5,
		batchDelayTimeMs: 20,
		batchMaxSize:     500 * 1024,
	}
	b.cond = NewCond(&b.mutex)
	b.queue = make([]*PendingProposal, 0)
	return b
}

func (b *ProposeBatch) Start() {
	b.batchID = NewGoID()
	go b.Run()
}

func (b *ProposeBatch) Run() {
	b.isStarted = true
	//daemon thread for very low qps.
	timeStat := NewTimeStat()

	for {
		b.mutex.Lock()
		defer b.mutex.Unlock()

		if b.isEnd {
			break
		}

		timeStat.Point()
		reqs := b.pluckProposal()
		b.mutex.Unlock()

		b.propose(reqs)

		b.mutex.Lock()
		passTime := timeStat.Point()
		needSleepTime := 0
		if passTime < b.batchDelayTimeMs {
			needSleepTime = b.batchDelayTimeMs - passTime
		}
		if b.needBatch() {
			needSleepTime = 0
		}
		if needSleepTime > 0 {
			b.cond.WaitFor(time.Duration(needSleepTime) * time.Millisecond)
		}
	}

	for !b.queue.Empty() {
		pendingProposal := b.queue.PopFront()
		pendingProposal.Notifier.SendNotify(ErrSystem)
	}

	log.Error("Ended.")
}

func (b *ProposeBatch) join() {

}

func (b *ProposeBatch) Stop() {
	if b.isStarted {
		b.mutex.Lock()

		b.isEnd = true
		b.cond.Broadcast()

		b.mutex.Unlock()

		b.join()
	}
}

func (b *ProposeBatch) Propose(value string, smCtx *SMCtx) (instanceID uint64, batchIndex uint32, err error) {
	if b.isEnd {
		return 0, 0, ErrSystem
	}

	notifier, err := b.notifierPool.GetNotifier(b.batchID)
	if err != nil {
		log.Error("get nofifier fail", log.Err(err))
		return 0, 0, ErrSystem
	}

	b.addProposal(value, &instanceID, &batchIndex, smCtx, notifier)
	err = notifier.WaitNotify()
	return instanceID, batchIndex, err
}

func (b *ProposeBatch) SetBatchCount(batchCount int) {
	b.batchCount = batchCount
}

func (b *ProposeBatch) SetBatchDelayTimeMs(batchDelayTimeMs int) {
	b.batchDelayTimeMs = batchDelayTimeMs
}

func (b *ProposeBatch) propose(reqs []*PendingProposal) {
	if len(reqs) == 0 {
		return
	}

	if len(reqs) == 1 {
		b.onlyOnePropose(reqs[0])
		return
	}

	batchValues := &BatchPaxosValues{}
	batchSMCtx := &BatchSMCtx{}
	for _, pendingProposal := range reqs {
		value := PaxosValue{SMID: 0, Value: []byte(*pendingProposal.Value)}
		if pendingProposal.SMCtx != nil {
			value.SMID = int32(pendingProposal.SMCtx.SMID)
		}
		batchValues.Values = append(batchValues.Values)
		batchSMCtx.SMCtxs = append(batchSMCtx.SMCtxs, pendingProposal.SMCtx)
	}

	smCtx := NewSMCtx(BatchProposeSMID, batchSMCtx)

	var (
		err        error
		buff       []byte
		instanceID uint64
	)

	buff, err = proto.Marshal(batchValues)
	if err == nil {
		instanceID, err = b.node.ProposeWithSMCtx(b.myGroupIdx, string(buff), smCtx)
		if err != nil {
			log.Error("real propose fail", log.Err(err))
		}
	} else {
		log.Error("batch values marshal fail", log.Err(err))
		err = ErrSystem
	}

	for i, pendingProposal := range reqs {
		*pendingProposal.BatchIndex = uint32(i)
		*pendingProposal.InstanceID = instanceID
		pendingProposal.Notifier.SendNotify(err)
	}
}

func (b *ProposeBatch) addProposal(value string, instanceID *uint64, batchIndex *uint32, smCtx *SMCtx, notifier *Notifier) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	pendingProposal := &PendingProposal{
		Notifier:       notifier,
		Value:          &value,
		SMCtx:          smCtx,
		InstanceID:     instanceID,
		BatchIndex:     batchIndex,
		AbsEnqueueTime: GetCurrentTimeMs(),
	}

	b.queue.PushBack(pendingProposal)
	if b.needBatch() {
		log.Info("direct batch.", log.Int("queue.size", b.queue.Size()))
		requests := b.pluckProposal()

		b.mutex.Unlock()
		b.propose(requests)
	}
}

func (b *ProposeBatch) pluckProposal() []*PendingProposal {
	reqs := make([]*PendingProposal, 0, b.queue.Size())
	pluckCount := 0

	for !b.queue.Empty() {
		pendingProposal := b.queue.PopFront()
		reqs = append(reqs, pendingProposal)

		pluckCount++
		if pluckCount >= b.batchCount {
			break
		}
	}

	log.Info("pluck request", log.Int("count", len(reqs)))
	return reqs
}

func (b *ProposeBatch) onlyOnePropose(pendingProposal *PendingProposal) {
	var err error
	*pendingProposal.InstanceID, err = b.node.ProposeWithSMCtx(b.myGroupIdx, *pendingProposal.Value, pendingProposal.SMCtx)
	pendingProposal.Notifier.SendNotify(err)
}

func (b *ProposeBatch) needBatch() bool {
	if b.queue.Size() >= b.batchCount {
		return true
	} else if b.queue.Size() > 0 {
		pendingProposal := b.queue.Front()
		nowTime := GetCurrentTimeMs()

		proposalPassTime := uint64(0)
		if nowTime > pendingProposal.AbsEnqueueTime {
			proposalPassTime = nowTime - pendingProposal.AbsEnqueueTime
		}

		if proposalPassTime > uint64(b.batchDelayTimeMs) {
			return true
		}
	}
	return false
}
