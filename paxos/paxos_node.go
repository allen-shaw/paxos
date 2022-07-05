package paxos

import (
	"encoding/binary"
	"errors"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/golang/protobuf/proto"
	"math"
)

// PaxosNode implement Node
type PaxosNode struct {
	groups        []*Group
	masters       []*MasterMgr
	proposeBatchs []*ProposeBatch

	defaultLogStorage *MultiDatabase
	defaultNetWork    NetWork
	notifierPool      *NotifierPool
	myNodeID          NodeID
}

func NewPaxosNode() *PaxosNode {
	return &PaxosNode{myNodeID: nilNode}
}

func (n *PaxosNode) Stop() {
	//1.step: must stop master(app) first.
	for _, master := range n.masters {
		master.StopMaster()
	}

	//2.step: stop proposebatch
	for _, proposeBatch := range n.proposeBatchs {
		proposeBatch.Stop()
	}

	//3.step: stop group
	for _, group := range n.groups {
		group.Stop()
	}

	//4. step: stop network.
	n.defaultNetWork.Stop()

	//5 .step: delete paxos instance.
	n.groups = nil
	//6. step: delete master state machine.
	n.masters = nil
	//7. step: delete proposebatch.
	n.proposeBatchs = nil
}

func (n *PaxosNode) Init(options *Options) (NetWork, error) {
	err := n.checkOptions(options)
	if err != nil {
		log.Error("check options fail", log.Err(err))
		return nil, err
	}

	n.myNodeID = options.MyNode.GetNodeId()

	//step1 init logstorage
	logStorage, err := n.initLogStorage(options)
	if err != nil {
		return nil, err
	}

	//step2 init network
	netWork, err := n.initNetWork(options)
	if err != nil {
		return nil, err
	}

	//step3 build masterlist
	for groupIdx := 0; groupIdx < options.GroupCount; groupIdx++ {
		master := NewMasterMgr(n, groupIdx, logStorage, options.MasterChangeCallback)
		n.masters = append(n.masters, master)

		err = master.Init()
		if err != nil {
			return netWork, err
		}
	}

	//step4 build grouplist
	for groupIdx := 0; groupIdx < options.GroupCount; groupIdx++ {
		masterSM := n.masters[groupIdx].GetMasterSM()
		group := NewGroup(logStorage, netWork, masterSM, groupIdx, options)
		n.groups = append(n.groups, group)
	}

	//step5 build batchpropose
	if options.UseBatchPropose {
		for groupIdx := 0; groupIdx < options.GroupCount; groupIdx++ {
			batch := NewProposeBatch(groupIdx, n, n.notifierPool)
			n.proposeBatchs = append(n.proposeBatchs, batch)
		}
	}

	//step6 init statemachine
	n.initStateMachine(options)

	//step7 parallel init group
	for _, group := range n.groups {
		group.StartInit()
	}

	for _, group := range n.groups {
		ret := group.GetInitRet()
		if ret != nil {
			err = ret
		}
	}

	if err != nil {
		return netWork, err
	}

	//last step. must init ok, then should start threads.
	//because that stop threads is slower, if init fail, we need much time to stop many threads.
	//so we put start threads in the last step.
	for _, group := range n.groups {
		group.Start()
	}

	n.runMaster(options)
	n.runProposeBatch()

	log.Info("init ok")
	return netWork, nil
}

func (n *PaxosNode) Propose(groupIdx int, value string) (uint64, error) {
	if !n.checkGroupID(groupIdx) {
		return 0, ErrGroupIdx
	}
	return n.groups[groupIdx].GetCommitter().NewValueGetID(value)
}

func (n *PaxosNode) ProposeWithSMCtx(groupIdx int, value string, smCtx *SMCtx) (uint64, error) {
	if !n.checkGroupID(groupIdx) {
		return 0, ErrGroupIdx
	}
	return n.groups[groupIdx].GetCommitter().NewValueGetIDWithSMCtx(value, smCtx)
}

func (n *PaxosNode) GetNowInstanceID(groupIdx int) uint64 {
	if !n.checkGroupID(groupIdx) {
		return math.MaxUint64
	}
	return n.groups[groupIdx].GetInstance().GetNowInstanceID()
}

func (n *PaxosNode) GetMinChosenInstanceID(groupIdx int) uint64 {
	if !n.checkGroupID(groupIdx) {
		return math.MaxUint64
	}
	return n.groups[groupIdx].GetInstance().GetMinChosenInstanceID()
}

// batch

func (n *PaxosNode) BatchPropose(groupIdx int, value string) (uint64, uint32, error) {
	return n.BatchProposeWithSMCtx(groupIdx, value, nil)
}

func (n *PaxosNode) BatchProposeWithSMCtx(groupIdx int, value string, smCtx *SMCtx) (uint64, uint32, error) {
	if !n.checkGroupID(groupIdx) {
		return 0, 0, ErrGroupIdx
	}
	if len(n.proposeBatchs) == 0 {
		return 0, 0, ErrSystem
	}

	return n.proposeBatchs[groupIdx].Propose(value, smCtx)
}

func (n *PaxosNode) SetBatchCount(groupIdx, batchCount int) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	if len(n.proposeBatchs) == 0 {
		return
	}

	n.proposeBatchs[groupIdx].SetBatchCount(batchCount)
}

func (n *PaxosNode) SetBatchDelayTimeMs(groupIdx, batchDelayTimeMs int) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	if len(n.proposeBatchs) == 0 {
		return
	}

	n.proposeBatchs[groupIdx].SetBatchDelayTimeMs(batchDelayTimeMs)
}

///////////////////////////////////////////////

func (n *PaxosNode) AddStateMachine(sm StateMachine) {
	for _, group := range n.groups {
		group.AddStateMachine(sm)
	}
}

func (n *PaxosNode) AddStateMachineWithGroupIdx(groupIdx int, sm StateMachine) {
	if !n.checkGroupID(groupIdx) {
		return
	}
	n.groups[groupIdx].AddStateMachine(sm)
}

func (n *PaxosNode) OnReceiveMessage(message string) error {
	if message == "" {
		log.Error("message nil, invalid")
		return ErrInvalidMsg
	}

	groupIdx := int(binary.BigEndian.Uint64([]byte(message[:GroupIdxLen])))

	if !n.checkGroupID(groupIdx) {
		log.Error("messgae group idx fail", log.Int("groupidx", groupIdx),
			log.Int("groupsize", len(n.groups)))
		return ErrGroupIdx
	}

	return n.groups[groupIdx].GetInstance().OnReceiveMessage([]byte(message))
}

func (n *PaxosNode) GetMyNodeID() NodeID {
	return n.myNodeID
}

func (n *PaxosNode) SetTimeoutMs(timeoutMs int) {
	for _, group := range n.groups {
		group.GetCommitter().SetTimeoutMs(timeoutMs)
	}
}

//////////////////////////////////////////////////////

func (n *PaxosNode) SetHoldPaxosLogCount(holdCount uint64) {
	for _, group := range n.groups {
		group.GetCheckpointCleaner().SetHoldPaxosLogCount(holdCount)
	}
}

func (n *PaxosNode) PauseCheckpointReplayer() {
	for _, group := range n.groups {
		group.GetCheckpointReplayer().Pause()
	}
}

func (n *PaxosNode) ContinueCheckpointReplayer() {
	for _, group := range n.groups {
		group.GetCheckpointReplayer().Continue()
	}
}
func (n *PaxosNode) PausePaxosLogCleaner() {
	for _, group := range n.groups {
		group.GetCheckpointCleaner().Pause()
	}
}
func (n *PaxosNode) ContinuePaxosLogCleaner() {
	for _, group := range n.groups {
		group.GetCheckpointCleaner().Continue()
	}
}

//////////////////////////////////////////////////////
//membership

func (n *PaxosNode) AddMember(groupIdx int, node *NodeInfo) error {
	if !n.checkGroupID(groupIdx) {
		return ErrGroupIdx
	}

	systemVSM := n.groups[groupIdx].GetConfig().GetSystemVSM()
	if systemVSM.GetGid() == 0 {
		return ErrMembershipOpNoGid
	}

	nodeInfos, version := systemVSM.GetMembership()

	for _, nodeInfo := range nodeInfos {
		if nodeInfo.GetNodeId() == node.GetNodeId() {
			return ErrMembershipOpAddNodeExist
		}
	}

	nodeInfos = append(nodeInfos, node)
	return n.proposalMembership(systemVSM, groupIdx, nodeInfos, version)
}

func (n *PaxosNode) RemoveMember(groupIdx int, node *NodeInfo) error {
	if !n.checkGroupID(groupIdx) {
		return ErrGroupIdx
	}
	systemVSM := n.groups[groupIdx].GetConfig().GetSystemVSM()
	if systemVSM.GetGid() == 0 {
		return ErrMembershipOpNoGid
	}
	nodeInfos, version := systemVSM.GetMembership()

	nodeExist := false
	afterNodeInfoList := make(NodeInfoList, 0)

	for _, nodeInfo := range nodeInfos {
		if node.GetNodeId() == node.GetNodeId() {
			nodeExist = true
		} else {
			afterNodeInfoList = append(afterNodeInfoList, nodeInfo)
		}
	}

	if !nodeExist {
		return ErrMembershipOpRemoveNodeNotExist
	}

	return n.proposalMembership(systemVSM, groupIdx, afterNodeInfoList, version)
}

func (n *PaxosNode) ChangeMember(groupIdx int, fromNode, toNode *NodeInfo) error {
	if !n.checkGroupID(groupIdx) {
		return ErrGroupIdx
	}
	systemVSM := n.groups[groupIdx].GetConfig().GetSystemVSM()
	if systemVSM.GetGid() == 0 {
		return ErrMembershipOpNoGid
	}
	nodeInfos, version := systemVSM.GetMembership()

	fromNodeExist := false
	toNodeExist := false
	afterNodeInfoList := make(NodeInfoList, 0)

	for _, nodeInfo := range nodeInfos {
		if nodeInfo.GetNodeId() == fromNode.GetNodeId() {
			fromNodeExist = true
			continue
		} else if nodeInfo.GetNodeId() == toNode.GetNodeId() {
			toNodeExist = true
			continue
		}

		afterNodeInfoList = append(afterNodeInfoList, nodeInfo)
	}

	if !fromNodeExist && toNodeExist {
		return ErrMembershipOpChangeNoChange
	}

	afterNodeInfoList = append(afterNodeInfoList, toNode)
	return n.proposalMembership(systemVSM, groupIdx, afterNodeInfoList, version)
}

func (n *PaxosNode) ShowMembership(groupIdx int) (NodeInfoList, error) {
	if !n.checkGroupID(groupIdx) {
		return nil, ErrGroupIdx
	}
	systemVSM := n.groups[groupIdx].GetConfig().GetSystemVSM()
	nodeInfos, _ := systemVSM.GetMembership()
	return nodeInfos, nil
}

//////////////////////////////////////////////////////

func (n *PaxosNode) GetMaster(groupIdx int) *NodeInfo {
	if !n.checkGroupID(groupIdx) {
		return NewNodeInfo(nilNode)
	}
	nodeID := n.masters[groupIdx].GetMasterSM().GetMaster()
	return NewNodeInfo(nodeID)
}

func (n *PaxosNode) GetMasterWithVersion(groupIdx int) (*NodeInfo, uint64) {
	if !n.checkGroupID(groupIdx) {
		return NewNodeInfo(nilNode), 0
	}
	nodeID, version := n.masters[groupIdx].GetMasterSM().GetMasterWithVersion()
	return NewNodeInfo(nodeID), version
}

func (n *PaxosNode) IsMaster(groupIdx int) bool {
	if !n.checkGroupID(groupIdx) {
		return false
	}
	return n.masters[groupIdx].GetMasterSM().IsIMMaster()
}

func (n *PaxosNode) SetMasterLease(groupIdx int, leaseTimeMs int) error {
	if !n.checkGroupID(groupIdx) {
		return ErrGroupIdx
	}
	n.masters[groupIdx].SetLeaseTime(leaseTimeMs)
	return nil
}

func (n *PaxosNode) DropMaster(groupIdx int) error {
	if !n.checkGroupID(groupIdx) {
		return ErrGroupIdx
	}
	n.masters[groupIdx].DropMaster()
	return nil
}

//////////////////////////////////////////////////////

func (n *PaxosNode) SetMaxHoldThreads(groupIdx int, maxHoldThreads int) {
	if !n.checkGroupID(groupIdx) {
		return
	}
	n.groups[groupIdx].GetCommitter().SetMaxHoldThreads(maxHoldThreads)
}

func (n *PaxosNode) SetProposeWaitTimeThresholdMS(groupIdx int, waitTimeThresholdMs int) {
	if !n.checkGroupID(groupIdx) {
		return
	}
	n.groups[groupIdx].GetCommitter().SetProposeWaitTimeThresholdMS(waitTimeThresholdMs)
}

func (n *PaxosNode) SetLogSync(groupIdx int, logSync bool) {
	if !n.checkGroupID(groupIdx) {
		return
	}
	n.groups[groupIdx].GetConfig().SetLogSync(logSync)
}

//////////////////////////////////////////////////////

func (n *PaxosNode) GetInstanceValue(groupIdx int, instanceID uint64) ([]*Pair[string, int], error) {
	if !n.checkGroupID(groupIdx) {
		return nil, ErrGroupIdx
	}

	value, smID, err := n.groups[groupIdx].GetInstance().GetInstanceValue(instanceID)
	if err != nil {
		return nil, err
	}

	values := make([]*Pair[string, int], 0)
	if smID == BatchProposeSMID {
		batchValues := &BatchPaxosValues{}
		err = proto.Unmarshal([]byte(value), batchValues)
		if err != nil {
			return nil, err
		}

		for _, value := range batchValues.Values {
			values = append(values, MakePair(string(value.Value), int(value.SMID)))
		}

	} else {
		values = append(values, MakePair(value, smID))
	}
	return values, nil
}

//////////////////////////////////////////////////////

func (n *PaxosNode) checkOptions(options *Options) error {
	if options.LogStorage == nil && options.LogStoragePath == "" {
		log.Error("no logpath and logstorage is nil")
		return ErrInvalidOptions
	}

	if options.GroupCount > 200 {
		log.Error("group count is too large", log.Int("group_count", options.GroupCount))
		return ErrInvalidOptions
	}

	if options.GroupCount <= 0 {
		log.Error("group count is invalid", log.Int("group_count", options.GroupCount))
		return ErrInvalidOptions
	}

	for _, nodeInfo := range options.FollowerNodeInfos {
		if nodeInfo.MyNode.GetNodeId() == nodeInfo.FollowNode.GetNodeId() {
			log.Error("node ip port equal to follow node",
				log.String("ip", nodeInfo.MyNode.GetIP()),
				log.Int("port", nodeInfo.MyNode.GetPort()))
			return ErrInvalidOptions
		}
	}

	for _, smInfo := range options.GroupSMInfos {
		if smInfo.GroupIdx >= options.GroupCount {
			log.Error("sm.groupidx large than group count",
				log.Int("sm.groupidx", smInfo.GroupIdx),
				log.Int("group_count", options.GroupCount))
			return ErrInvalidOptions
		}
	}

	return nil
}

func (n *PaxosNode) initLogStorage(options *Options) (LogStorage, error) {
	if options.LogStorage != nil {
		log.Info("ok, use user logstorage")
		return options.LogStorage, nil
	}

	if options.LogStoragePath == "" {
		log.Error("logstorage path is nil")
		return nil, errors.New("logstorage path is nil")
	}

	err := n.defaultLogStorage.Init(options.LogStoragePath, options.GroupCount)
	if err != nil {
		log.Error("init default logstorage fail", log.Err(err),
			log.String("logpath", options.LogStoragePath))
		return nil, err
	}

	log.Info("ok, use default logstorage")
	return n.defaultLogStorage, nil
}

func (n *PaxosNode) initNetWork(options *Options) (NetWork, error) {
	if options.NetWork != nil {
		log.Info("ok, use user network")
		return options.NetWork, nil
	}

	err := n.defaultNetWork.Init(options.MyNode.GetIP(), options.MyNode.GetPort())
	if err != nil {
		log.Error("init default network fail",
			log.String("listen_ip", options.MyNode.GetIP()),
			log.Int("listen_port", options.MyNode.GetPort()),
			log.Err(err))
		return nil, err
	}

	log.Info("ok, use default network")

	return n.defaultNetWork, nil
}

func (n *PaxosNode) initMaster(options *Options) error {
	return nil
}

func (n *PaxosNode) initStateMachine(options *Options) {
	for _, smInfo := range options.GroupSMInfos {
		for _, sm := range smInfo.SMList {
			n.AddStateMachineWithGroupIdx(smInfo.GroupIdx, sm)
		}
	}
}

func (n *PaxosNode) checkGroupID(groupIdx int) bool {
	if groupIdx < 0 || groupIdx > len(n.groups) {
		return false
	}
	return true
}

func (n *PaxosNode) runMaster(options *Options) {
	for _, groupSMInfo := range options.GroupSMInfos {
		//check if need to run master.
		if groupSMInfo.IsUseMaster {
			if !n.groups[groupSMInfo.GroupIdx].GetConfig().IsIMFollower() {
				n.masters[groupSMInfo.GroupIdx].Run()
			} else {
				log.Error("I'm follower, not run master damon.")
			}
		}
	}
}

func (n *PaxosNode) runProposeBatch() {
	for _, batch := range n.proposeBatchs {
		batch.Start()
	}
}

func (n *PaxosNode) proposalMembership(systemVSM *SystemVSM, groupIdx int, nodeInfos NodeInfoList, version uint64) error {
	opValue, err := systemVSM.MembershipOPValue(nodeInfos, version)
	if err != nil {
		return ErrSystem
	}

	var smRet error
	smCtx := NewSMCtx(SystemVSMID, smRet)

	_, err = n.ProposeWithSMCtx(groupIdx, opValue, smCtx)
	if err != nil {
		return err
	}

	return smRet
}
