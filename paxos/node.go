package paxos

type Node interface {
	// Base function.
	Propose(groupIdx int, value string) (instanceID uint64, err error)
	ProposeWithSMCtx(groupIdx int, value string, ctx *SMCtx) (instanceID uint64, err error)
	GetNowInstanceID(groupIdx int) uint64
	GetMinChosenInstanceID(groupIdx int) uint64
	GetMyNodeID() NodeID

	// Batch propose.
	//Only set options::UserBatchPropose as true can use this batch API.
	//Warning: BatchProposal will have same InstanceID returned but different BatchIndex.
	//Batch values's execute order in StateMachine is certain, the return value BatchIndex
	//means the execute order index, start from 0.
	BatchPropose(groupIdx int, value string) (instanceID uint64, batchIndex uint32, err error)
	BatchProposeWithSMCtx(groupIdx int, value string, ctx *SMCtx) (instanceID uint64, batchIndex uint32, err error)

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
	ShowMembership(groupIdx int) (nodeInfos NodeInfoList, err error)

	//Add a paxos node to membership.
	AddMember(groupIdx int, node *NodeInfo) error

	//Remove a paxos node from membership.
	RemoveMember(groupIdx int, node *NodeInfo) error

	//Change membership by one node to another node.
	ChangeMember(groupIdx int, fromNode, toNode *NodeInfo) error

	//Master

	//Check who is master.
	GetMaster(groupIdx int) *NodeInfo

	//Check who is master and get version.
	GetMasterWithVersion(groupIdx int) (nodeInfo *NodeInfo, version uint64)

	//Check is i'm master.
	IsMaster(groupIdx int) bool
	SetMasterLease(groupIdx int, leaseTimeMs int) error
	DropMaster(groupIdx int) error

	//Qos

	//If many goroutine propose same group, that some goroutine will be on waiting status.
	//Set max hold threads, and we will reject some propose request to avoid to many goroutines be holded.
	//Reject propose request will get retcode(PaxosTryCommitRet_TooManyThreadWaiting_Reject), check on def.h.
	SetMaxHoldThreads(groupIdx int, maxHoldThreads int)

	//To avoid threads be holded too long time, we use this threshold to reject some propose to control thread's wait time.
	SetProposeWaitTimeThresholdMS(groupIdx int, waitTimeThresholdMs int)

	//write disk
	SetLogSync(groupIdx int, logSync bool)

	OnReceiveMessage(message string) error
}

func RunNode(options *Options) (Node, error) {
	if options.IsLargeValueMode {
		InsideOptionsInstance().SetAsLargeBufferMode()
	}
	InsideOptionsInstance().SetGroupCount(options.GroupCount)

	realNode := NewPaxosNode()
	network, err := realNode.Init(options)
	if err != nil {
		return nil, err
	}

	//step1 set node to network
	//very important, let network on recieve callback can work.
	network.SetNode(realNode)

	//step2 run network.
	//start recieve message from network, so all must init before this step.
	//must be the last step.
	err = network.Run()
	if err != nil {
		return nil, err
	}

	return realNode, nil
}
