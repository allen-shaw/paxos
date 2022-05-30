package paxos

import (
	"encoding/binary"
	"net"
)

type NodeID uint64

const nilNode NodeID = 0

type NodeInfo struct {
	nodeID NodeID
	ip     string
	port   int
}

func NewNodeInfo(nodeID NodeID) *NodeInfo {
	i := &NodeInfo{nodeID: nodeID}
	i.makeNodeID()
	return i
}

func NewNodeInfoWithAddr(ip string, port int) *NodeInfo {
	i := &NodeInfo{ip: ip, port: port}
	i.parseNodeID()
	return i
}

func (i *NodeInfo) SetNodeID(nodeID NodeID) {
	i.nodeID = nodeID
	i.parseNodeID()
}

func (i *NodeInfo) GetNodeId() NodeID {
	return i.nodeID
}

func (i *NodeInfo) SetIPPort(ip string, port int) {
	i.ip = ip
	i.port = port
	i.makeNodeID()
}

func (i *NodeInfo) GetIP() string {
	return i.ip
}

func (i *NodeInfo) GetPort() int {
	return i.port
}

func (i *NodeInfo) makeNodeID() {
	ip := net.ParseIP(i.ip)
	i.nodeID = NodeID(binary.BigEndian.Uint64(ip)<<32 | uint64(i.port))
}

func (i *NodeInfo) parseNodeID() {
	i.port = int(i.nodeID & 0xffffffff)
	ipBuff := make([]byte, 0, 4)
	binary.BigEndian.PutUint32(ipBuff, uint32(i.nodeID>>32))
	i.ip = net.IPv4(ipBuff[0], ipBuff[1], ipBuff[2], ipBuff[3]).String()
}

type FollowerNodeInfo struct {
	MyNode     NodeInfo
	FollowNode NodeInfo
}

type NodeInfoList []*NodeInfo
type FollowerNodeInfoList []FollowerNodeInfo

type GroupSMInfo struct {

	//required
	//GroupIdx interval is [0, iGroupCount)
	GroupIdx int

	//optional
	//One paxos group can mounting multi state machines.
	SMList []*StateMachine

	//optional
	//Master election is an internal state machine.
	//Set IsUseMaster as true to open master election feature.
	//Default is false.
	IsUseMaster bool
}

func NewGroupSMInfo() *GroupSMInfo {
	return &GroupSMInfo{GroupIdx: -1, IsUseMaster: false}
}

type GroupSMInfoList []GroupSMInfo

type MembershipChangeCallback func(int, NodeInfoList)
type MasterChangeCallback func(int, *NodeInfo, uint64)

type Options struct {
	//optional
	//User-specified paxoslog storage.
	//Default is nullptr.
	LogStorage LogStorage

	//optional
	//If poLogStorage == nullptr, sLogStoragePath is required.
	LogStoragePath string

	//optional
	//If true, the write will be flushed from the operating system
	//buffer cache before the write is considered complete.
	//If this flag is true, writes will be slower.
	//If this flag is false, and the machine crashes, some recent
	//writes may be lost. Note that if it is just the process that
	//crashes (i.e., the machine does not reboot), no writes will be
	//lost even if sync==false. Because of the data lost, we not guarantee consistence.
	//
	//Default is true.
	Sync bool

	//optional
	//Default is 0.
	//This means the write will skip flush at most iSyncInterval times.
	//That also means you will lost at most iSyncInterval count's paxos log.
	SyncInterval int

	//optional
	//User-specified network.
	//NetWork NetWork;
	NetWork NetWork

	//optional
	//We support to run multi paxos on one process.
	//One paxos group here means one independent paxos. Any two paxos(paxos group) only share network, no other.
	//There is no communication between any two paxos group.
	//Default is 1.
	GroupCount int

	//required
	//Self node's ip/port.
	MyNode NodeInfo

	//required
	//All nodes's ip/port with a paxos set(usually three or five nodes).
	NodeInfoList NodeInfoList

	//optional
	//Only UseMembership == true, we use option's nodeinfolist to init paxos membership,
	//after that, paxos will remember all nodeinfos, so second time you can run paxos without NodeList,
	//and you can only change membership by use function ChangeMember.
	//
	//Default is false.
	//if UseMembership == false, that means every time you run paxos will use NodeList to build a new membership.
	//when you change membership by a new NodeList, we don't guarantee consistence.
	//
	//For test, you can set false.
	//But when you use it to real services, remember to set true.
	UseMembership bool

	//While membership change, paxos will call this function.
	//Default is nullptr.
	MembershipChangeCallback MembershipChangeCallback

	//While master change, paxos will call this function.
	//Default is nullptr.
	MasterChangeCallback MasterChangeCallback

	//optional
	//One phxpaxos can mounting multi state machines.
	//This vector include different phxpaxos's state machines list.
	GroupSMInfos GroupSMInfoList

	//optional
	Monitor Monitor

	//optional
	//If use this mode, that means you propose large value(maybe large than 5M means large) much more.
	//Large value means long latency, long timeout, this mode will fit it.
	//Default is false
	IsLargeValueMode bool

	//optional
	//All followers's ip/port, and follow to node's ip/port.
	//Follower only learn but not participation paxos algorithmic process.
	//Default is empty.
	FollowerNodeInfos FollowerNodeInfoList

	//optional
	//If you use checkpoint replayer feature, set as true.
	//Default is false;
	UseCheckpointReplayer bool

	//optional
	//Only bUseBatchPropose is true can use API BatchPropose
	//Default is false;
	UseBatchPropose bool

	//optional
	//Only OpenChangeValueBeforePropose is true, that will callback sm's function(BeforePropose).
	//Default is false;
	OpenChangeValueBeforePropose bool
}

func NewOptions() *Options {
	return &Options{
		Sync:                         true,
		SyncInterval:                 0,
		GroupCount:                   1,
		UseMembership:                false,
		IsLargeValueMode:             false,
		UseCheckpointReplayer:        false,
		UseBatchPropose:              false,
		OpenChangeValueBeforePropose: false,
	}
}
