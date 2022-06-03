package paxos

type Node interface {
	// Base function.
	Propose(groupIdx int, value string, instanceID uint64) error
	ProposeWithCtx(groupIdx int, value string, instanceID uint64, ctx *SMCtx) error
	GetNowInstanceID(groupIdx int) uint64
	GetMinChosenInstanceID(groupIdx int) uint64
	GetMyNodeID() NodeID

	// Batch propose.
	//Only set options::UserBatchPropose as true can use this batch API.
	//Warning: BatchProposal will have same InstanceID returned but different BatchIndex.
	//Batch values's execute order in StateMachine is certain, the return value BatchIndex
	//means the execute order index, start from 0.
	BatchPropose(groupIdx int, value string, instanceID uint64, batchIndex uint32) int
	BatchProposeWithCtx(groupIdx int, value string, instanceID uint64, batchIndex uint32, ctx *SMCtx) int

	//Paxos will batch proposal while waiting proposals count reach to BatchCount,
	//or wait time reach to BatchDelayTimeMs.
	SetBatchCount(groupIdx int, batchCount int)
	SetBatchDelayTimeMs(groupIdx int, batchDelayTimeMs int)

	//State machine.

	//This function will add state machine to all group.
	AddStateMachine(sm StateMachine)
	AddStateMachineWithGroupIdx(groupIdx int, sm StateMachine)

	//Timeout control.
	SetTimeoutMs(timeoutMs int)

	//Set the number you want to keep paxoslog's count.
	//We will only delete paxoslog before checkpoint instanceid.
	//If HoldCount < 300, we will set it to 300. Not suggest too small holdcount.
	SetHoldPaxosLogCount(holdCount uint64)

	//Replayer is to help sm make checkpoint.
	//Checkpoint replayer default is paused, if you not use this, ignord this function.
	//If sm use ExecuteForCheckpoint to make checkpoint, you need to run replayer(you can run in any time).

	//Pause checkpoint replayer.
	PauseCheckpointReplayer()

	//Continue to run replayer
	ContinueCheckpointReplayer()

	//Paxos log cleaner working for deleting paxoslog before checkpoint instanceid.
	//Paxos log cleaner default is pausing.

	//pause paxos log cleaner.
	PausePaxosLogCleaner()

	//Continue to run paxos log cleaner.
	ContinuePaxosLogCleaner()

	//Membership

	//Show now membership.
	//virtual int ShowMembership(const int iGroupIdx, NodeInfoList & vecNodeInfoList) = 0;
	ShowMembership(groupIdx int, nodeInfos NodeInfoList) int

	//Add a paxos node to membership.
	AddMember(groupIdx int, node *NodeInfo) int

	//Remove a paxos node from membership.
	RemoveMember(groupIdx int, node *NodeInfo) int

	//Change membership by one node to another node.
	ChangeMember(groupIdx int, fromNode, toNode *NodeID) int

	//Master

	//Check who is master.
	GetMaster(groupIdx int) NodeInfo

	//Check who is master and get version.
	GetMasterWithVersion(groupIdx int, version uint64) NodeInfo

	//Check is i'm master.
	IsMaster(groupIdx int) bool
	SetMasterLease(groupIdx int, leaseTimeMs int) int
	DropMaster(groupIdx int) int

	//Qos

	//If many goroutine propose same group, that some goroutine will be on waiting status.
	//Set max hold threads, and we will reject some propose request to avoid to many goroutines be holded.
	//Reject propose request will get retcode(PaxosTryCommitRet_TooManyThreadWaiting_Reject), check on def.h.
	SetMaxHoldThreads(groupIdx int, maxHoldThreads int)

	//To avoid threads be holded too long time, we use this threshold to reject some propose to control thread's wait time.
	SetProposeWaitTimeThresholdMS(groupIdx int, waitTimeThresholdMs int)

	//write disk
	SetLogSync(groupIdx int, logSync bool)

	onReceiveMessage(message string) int
}

func RunNode(options *Options) (Node, error) {
	if options.IsLargeValueMode {
		InsideOptionsInstance().SetAsLargeBufferMode()
	}
	InsideOptionsInstance().SetGroupCount(options.GroupCount)

	//   Breakpoint::m_poBreakpoint = nullptr;
	//    BP->SetInstance(oOptions.poBreakpoint);
	//realNode := newPaxosNode()
}
