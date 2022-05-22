package paxos

type MsgCounter struct {
	config                    *Config
	receiveMsgNodeIDs         map[NodeID]struct{}
	rejectMsgNodeIDs          map[NodeID]struct{}
	promiseOrAcceptMsgNodeIDs map[NodeID]struct{}
}

func NewMsgCounter(config *Config) *MsgCounter {
	c := &MsgCounter{config: config}
	c.StartNewRound()
	return c
}

func (c *MsgCounter) StartNewRound() {
	c.receiveMsgNodeIDs = make(map[NodeID]struct{})
	c.rejectMsgNodeIDs = make(map[NodeID]struct{})
	c.promiseOrAcceptMsgNodeIDs = make(map[NodeID]struct{})
}

func (c *MsgCounter) AddReceive(nodeID NodeID) {
	c.receiveMsgNodeIDs[nodeID] = struct{}{}
}

func (c *MsgCounter) AddReject(nodeID NodeID) {
	c.rejectMsgNodeIDs[nodeID] = struct{}{}
}

func (c *MsgCounter) AddPromiseOrAccept(nodeID NodeID) {
	c.promiseOrAcceptMsgNodeIDs[nodeID] = struct{}{}
}

func (c *MsgCounter) IsPassedOnThisRound() bool {
	return len(c.promiseOrAcceptMsgNodeIDs) >= c.config.GetMajorityCount()
}

func (c *MsgCounter) IsRejectedOnThisRound() bool {
	return len(c.rejectMsgNodeIDs) >= c.config.GetMajorityCount()
}

func (c *MsgCounter) IsAllReceiveOnThisRound() bool {
	return len(c.receiveMsgNodeIDs) == c.config.GetNodeCount()
}
