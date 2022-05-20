package paxos

import "github.com/AllenShaw19/paxos/log"

type Config struct {
	logSync       bool
	syncInterval  int
	useMembership bool

	myNodeID   NodeID
	nodeCount  int
	myGroupIdx int
	groupCount int

	nodeInfos NodeInfoList

	isFollower     bool
	followToNodeID NodeID

	systemVSM *SystemVSM
	masterSM  *InsideSM

	tmpNodeOnlyForLearn map[NodeID]uint64
	myFollower          map[NodeID]uint64
}

func NewConfig(logStorage LogStorage,
	logSync bool,
	syncInterval int,
	useMembership bool,
	myNode *NodeInfo,
	nodeInfos NodeInfoList,
	followerNodeInfos FollowerNodeInfoList,
	myGroupIdx int,
	groupCount int,
	callback MembershipChangeCallback) *Config {
	c := &Config{}

	c.logSync = logSync
	c.syncInterval = syncInterval
	c.useMembership = useMembership
	c.myNodeID = myNode.GetNodeID()
	c.nodeCount = len(nodeInfos)
	c.myGroupIdx = myGroupIdx
	c.groupCount = groupCount
	c.systemVSM = NewSystemVSM(myGroupIdx, myNode.GetNodeID(), logStorage, callback)
	c.masterSM = nil

	c.nodeInfos = nodeInfos
	c.isFollower = false
	c.followToNodeID = nilNode

	for _, followerNodeInfo := range followerNodeInfos {
		followerNodeID := followerNodeInfo.MyNode.GetNodeID()
		if followerNodeID == myNode.GetNodeID() {
			log.Info("Iam follower.",
				log.String("IP", myNode.GetIP()),
				log.Int("Port", myNode.GetPort()),
				log.Int("nodeid", myNode.GetNodeID()))
			c.isFollower = true
			c.followToNodeID = followerNodeID
			InsideOptionsInstance().SetAsFollower()
		}
	}

	return c
}

func (c *Config) Init() error {
	err := c.systemVSM.Init()
	if err != nil {
		log.Error("config init fail.", log.Err(err))
		return err
	}
	c.systemVSM.AddNodeIDList(c.nodeInfos)
	log.Info("config init success.")
	return nil
}

func (c *Config) Check() bool {

}

func (c *Config) GetGid() int64 {

}

func (c *Config) GetMyNodeID() NodeID {

}

func (c *Config) GetNodeCount() int {}

func (c *Config) GetMyGroupIdx() int {}

func (c *Config) GetGroupCount() int {}

func (c *Config) GetMajorityCount() int {}

func (c *Config) GetIsUseMembership() int {}

func (c *Config) GetPrepareTimeoutMs() int {}

func (c *Config) GetAcceptTimeoutMs() int {}

func (c *Config) GetAskForLearnTimeoutMs() uint64 {}

func (c *Config) IsValidNodeID(nodeID NodeID) bool {}

func (c *Config) IsIMFollower() bool {}

func (c *Config) GetFollowToNodeID() NodeID {}

func (c *Config) LogSync() bool {}

func (c *Config) SyncInterval() int {}

func (c *Config) SetLogSync(logSync bool) bool {}

func (c *Config) GetSystemVSM() *SystemVSM {
	return c.systemVSM
}

func (c *Config) SetMasterSM(masterSM *InsideSM) {

}

func (c *Config) GetMasterSM() *InsideSM {

}

func (c *Config) AddTmpNodeOnlyForLearn(tmpNodeID NodeID) {

}

// GetTmpNodeMap this function only for communicate.
func (c *Config) GetTmpNodeMap() map[NodeID]uint64 {

}

func (c *Config) AddFollowerNode(followerNodeID NodeID) {

}

func (c *Config) GetMyFollowerMap() map[NodeID]uint64 {

}

func (c *Config) GetMyFollowerCount() int {

}
