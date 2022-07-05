package paxos

import (
	"github.com/AllenShaw19/paxos/plugin/log"
)

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
	masterSM  InsideSM

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
	c.myNodeID = myNode.GetNodeId()
	c.nodeCount = len(nodeInfos)
	c.myGroupIdx = myGroupIdx
	c.groupCount = groupCount
	c.systemVSM = NewSystemVSM(myGroupIdx, myNode.GetNodeId(), logStorage, callback)
	c.masterSM = nil

	c.nodeInfos = nodeInfos
	c.isFollower = false
	c.followToNodeID = nilNode

	for _, followerNodeInfo := range followerNodeInfos {
		followerNodeID := followerNodeInfo.MyNode.GetNodeId()
		if followerNodeID == myNode.GetNodeId() {
			log.Info("Iam follower.",
				log.String("ip", myNode.ip),
				log.Int("port", myNode.port),
				log.Uint64("nodeid", uint64(myNode.GetNodeId())))
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
	if !c.systemVSM.IsIMInMembership() {
		log.Error("my node is not in membership", log.Uint64("my_node", uint64(c.myNodeID)))
		return false
	}
	return true
}

func (c *Config) GetGid() uint64 {
	return c.systemVSM.GetGid()
}

func (c *Config) GetMyNodeID() NodeID {
	return c.myNodeID
}

func (c *Config) GetNodeCount() int {
	return c.systemVSM.GetNodeCount()
}

func (c *Config) GetMyGroupIdx() int {
	return c.myGroupIdx
}

func (c *Config) GetGroupCount() int {
	return c.groupCount
}

func (c *Config) GetMajorityCount() int {
	return c.systemVSM.GetMajorityCount()
}

func (c *Config) GetIsUseMembership() bool {
	return c.useMembership
}

func (c *Config) GetPrepareTimeoutMs() int {
	return 3000
}

func (c *Config) GetAcceptTimeoutMs() int {
	return 3000
}

func (c *Config) GetAskForLearnTimeoutMs() uint64 {
	return 2000
}

func (c *Config) IsValidNodeID(nodeID NodeID) bool {
	return c.systemVSM.IsValidNodeID(nodeID)
}

func (c *Config) IsIMFollower() bool {
	return c.isFollower
}

func (c *Config) GetFollowToNodeID() NodeID {
	return c.followToNodeID
}

func (c *Config) LogSync() bool {
	return c.logSync
}

func (c *Config) SyncInterval() int {
	return c.syncInterval
}

func (c *Config) SetLogSync(logSync bool) {
	c.logSync = logSync
}

func (c *Config) GetSystemVSM() *SystemVSM {
	return c.systemVSM
}

func (c *Config) SetMasterSM(masterSM InsideSM) {
	c.masterSM = masterSM
}

func (c *Config) GetMasterSM() InsideSM {
	return c.masterSM
}

const TmpNodeTimeout = 60000

func (c *Config) AddTmpNodeOnlyForLearn(tmpNodeID NodeID) {
	nodeIDs := c.systemVSM.GetMembershipMap()
	if _, ok := nodeIDs[tmpNodeID]; ok {
		return
	}
	c.tmpNodeOnlyForLearn[tmpNodeID] = GetCurrentTimeMs() + TmpNodeTimeout
}

// GetTmpNodeMap this function only for communicate.
func (c *Config) GetTmpNodeMap() map[NodeID]uint64 {
	nowTime := GetCurrentTimeMs()
	for nodeID, lastAddTime := range c.tmpNodeOnlyForLearn {
		if lastAddTime < nowTime {
			log.Error("tmp node timeout", log.Uint64("tmpnode", uint64(nodeID)),
				log.Uint64("nowtime", nowTime), log.Uint64("tmpnode_last_add_time", lastAddTime))

			delete(c.tmpNodeOnlyForLearn, nodeID)
		}
	}
	return c.tmpNodeOnlyForLearn
}

func (c *Config) AddFollowerNode(followerNodeID NodeID) {
	followerTimeout := uint64(AskForLearnNoopInterval() * 3)
	c.myFollower[followerNodeID] = GetCurrentTimeMs() + followerTimeout
}

func (c *Config) GetMyFollowerMap() map[NodeID]uint64 {
	nowTime := GetCurrentTimeMs()
	for nodeID, lastAddTime := range c.myFollower {
		if lastAddTime < nowTime {
			log.Error("follower timeout", log.Uint64("follower", uint64(nodeID)),
				log.Uint64("nowtime", nowTime), log.Uint64("follower_last_add_time", lastAddTime))
			delete(c.myFollower, nodeID)
		}
	}

	return c.myFollower
}

func (c *Config) GetMyFollowerCount() int {
	return len(c.myFollower)
}
