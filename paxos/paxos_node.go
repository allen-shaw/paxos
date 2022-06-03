package paxos

// PaxosNode implement Node
type PaxosNode struct {
	groups       []*Group
	masters      []*MasterMgr
	proposeBatch []*ProposeBatch

	defaultLogStorage *MultiDatabase
	defaultNetWork    NetWork
	notifierPool      *NotifierPool
	myNodeID          NodeID
}
