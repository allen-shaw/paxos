package paxos

import (
	"errors"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/golang/protobuf/proto"
	"math"
)

type SystemVSM struct {
	myGroupIdx      int
	systemVariables *SystemVariables
	systemVStore    *SystemVariablesStore

	nodeIDs  map[NodeID]struct{}
	myNodeID NodeID

	membershipChangeCallback MembershipChangeCallback
}

func (sm *SystemVSM) LockCheckpointState() error {
	return nil
}

func (sm *SystemVSM) GetCheckpointState(groupIdx int) (dirPath string, files []string, err error) {
	return "", nil, nil
}

func (sm *SystemVSM) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, files []string, checkpointInstanceID uint64) error {
	return nil
}

func (sm *SystemVSM) BeforePropose(groupIdx int, value string) (string, error) {
	return "", nil
}

func (sm *SystemVSM) NeedCallBeforePropose() bool {
	return false
}

func NewSystemVSM(groupIdx int,
	myNodeID NodeID,
	logStorage LogStorage,
	callback MembershipChangeCallback) *SystemVSM {
	vsm := &SystemVSM{}
	vsm.myGroupIdx = groupIdx
	vsm.systemVStore = NewSystemVariablesStore(logStorage)
	vsm.myNodeID = myNodeID
	vsm.membershipChangeCallback = callback
	return vsm
}

func (sm *SystemVSM) Init() error {
	var err error
	sm.systemVariables, err = sm.systemVStore.Read(sm.myGroupIdx)
	if err != nil && err != ErrNotExist {
		return err
	}
	if err == ErrNotExist {
		sm.systemVariables.Gid = 0
		sm.systemVariables.Version = math.MaxUint64
		log.Info("variables not exists")
	} else {
		sm.RefleshNodeID()
		log.Info("init succ.", log.Int("groupidx", sm.myGroupIdx),
			log.Uint64("gid", sm.systemVariables.Gid),
			log.Uint64("version", sm.systemVariables.Version))
	}
	return nil
}

func (sm *SystemVSM) Execute(groupIdx int, instanceID uint64, value string, smCtx *SMCtx) bool {
	variables := &SystemVariables{}
	err := proto.Unmarshal([]byte(value), variables)
	if err != nil {
		log.Error("system variables unmarshal fail", log.Err(err))
		return false
	}

	var smRet *int
	if smCtx != nil && smCtx.Ctx != nil {
		smRet = smCtx.Ctx.(*int)
	}

	if sm.systemVariables.Gid != 0 && variables.Gid != sm.systemVariables.Gid {
		log.Error("modify.gid not equal to now.gid",
			log.Uint64("modify.gid", variables.Gid),
			log.Uint64("now.gid", sm.systemVariables.Gid))

		if smRet != nil {
			*smRet = PaxosMembershipOpGidNotSame
		}
		return true
	}

	if variables.Version != sm.systemVariables.Version {
		log.Error("modify.version not equal to now.version",
			log.Uint64("modify.version", variables.Version),
			log.Uint64("now.version", sm.systemVariables.Version))
		if smRet != nil {
			*smRet = PaxosMembershipOpVersionConflit
		}
		return true
	}

	variables.Version = instanceID
	err = sm.UpdateSystemVariables(variables)
	if err != nil {
		return false
	}
	log.Info("OK", log.Uint64("new_version", sm.systemVariables.Version),
		log.Uint64("gid", sm.systemVariables.Gid))

	if smRet != nil {
		*smRet = 0
	}
	return true
}

func (sm *SystemVSM) SMID() int {
	return SystemVSMID
}

func (sm *SystemVSM) GetGid() uint64 {
	return sm.systemVariables.Gid
}

func (sm *SystemVSM) GetMembership() (nodeInfoList NodeInfoList, version uint64) {
	version = sm.systemVariables.Version
	nodeInfoList = make(NodeInfoList, 0, len(sm.systemVariables.MemberShip))

	for _, nodeInfo := range sm.systemVariables.MemberShip {
		tmpNode := NewNodeInfo(NodeID(nodeInfo.NodeId))
		nodeInfoList = append(nodeInfoList, tmpNode)
	}

	return nodeInfoList, version
}

func (sm *SystemVSM) CreateGidOPValue(gid uint64) (string, error) {
	variable := *sm.systemVariables
	variable.Gid = gid

	buff, err := proto.Marshal(&variable)
	if err != nil {
		log.Error("system variable marshal fail", log.Err(err))
		return "", err
	}

	return string(buff), nil
}

func (sm *SystemVSM) MembershipOPValue(nodeInfoList NodeInfoList, version uint64) (string, error) {
	variables := &SystemVariables{}
	variables.Version = version
	variables.Gid = sm.systemVariables.Gid

	for _, nodeInfo := range nodeInfoList {
		paxosNodeInfo := &PaxosNodeInfo{
			Rid:    0,
			NodeId: uint64(nodeInfo.GetNodeId()),
		}
		variables.MemberShip = append(variables.MemberShip, paxosNodeInfo)
	}

	value, err := proto.Marshal(variables)
	if err != nil {
		log.Error("system variable marshal fail", log.Err(err))
		return "", err
	}

	return string(value), nil
}

//membership

func (sm *SystemVSM) AddNodeIDList(nodeInfos NodeInfoList) {
	if sm.systemVariables.Gid != 0 {
		log.Error("No need to add, already have membership info.")
		return
	}

	sm.nodeIDs = make(map[NodeID]struct{})
	sm.systemVariables.MemberShip = make([]*PaxosNodeInfo, 0)

	for _, nodeInfo := range nodeInfos {
		paxosNodeInfo := &PaxosNodeInfo{
			Rid:    0,
			NodeId: uint64(nodeInfo.GetNodeId()),
		}
		sm.systemVariables.MemberShip = append(sm.systemVariables.MemberShip, paxosNodeInfo)
	}

	sm.RefleshNodeID()
}

func (sm *SystemVSM) RefleshNodeID() {
	sm.nodeIDs = make(map[NodeID]struct{})

	nodeInfoList := make(NodeInfoList, 0, len(sm.systemVariables.MemberShip))

	for _, nodeInfo := range sm.systemVariables.MemberShip {
		tmpNode := NewNodeInfo(NodeID(nodeInfo.GetNodeId()))

		log.Info("node info", log.String("ip", tmpNode.GetIP()),
			log.Int("port", tmpNode.GetPort()),
			log.Uint64("nodeid", uint64(tmpNode.GetNodeId())))

		sm.nodeIDs[tmpNode.GetNodeId()] = struct{}{}
		nodeInfoList = append(nodeInfoList, tmpNode)
	}

	if sm.membershipChangeCallback != nil {
		sm.membershipChangeCallback(sm.myGroupIdx, nodeInfoList)
	}
}

func (sm *SystemVSM) GetNodeCount() int {
	return len(sm.nodeIDs)
}

func (sm *SystemVSM) GetMajorityCount() int {
	return sm.GetNodeCount()/2 + 1
}

func (sm *SystemVSM) IsValidNodeID(nodeID NodeID) bool {
	if sm.systemVariables.Gid == 0 {
		return true
	}
	_, ok := sm.nodeIDs[nodeID]
	return ok
}

func (sm *SystemVSM) IsIMInMembership() bool {
	_, ok := sm.nodeIDs[sm.myNodeID]
	return ok
}

func (sm *SystemVSM) GetMembershipMap() map[NodeID]struct{} {
	return sm.nodeIDs
}

///////////////////////////////////////////////////////////////////////////////

func (sm *SystemVSM) GetCheckpointInstanceID(groupIdx int) uint64 {
	return sm.systemVariables.Version
}

func (sm *SystemVSM) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue string) bool {
	return true
}

func (sm *SystemVSM) UnLockCheckpointState() {}

///////////////////////////////////////////////////////////////////////////////

func (sm *SystemVSM) GetCheckpointBuffer() (string, error) {
	if sm.systemVariables.Version == math.MaxUint64 ||
		sm.systemVariables.Gid == 0 {
		return "", nil
	}

	buff, err := proto.Marshal(sm.systemVariables)
	if err != nil {
		log.Error("system variables serialize fail", log.Err(err))
		return "", err
	}
	return string(buff), nil
}

func (sm *SystemVSM) UpdateByCheckpoint(buff []byte) (changed bool, err error) {
	if len(buff) == 0 {
		return false, nil
	}

	changed = false

	variables := &SystemVariables{}
	err = proto.Unmarshal(buff, variables)
	if err != nil {
		log.Error("unmarshal buffer fail", log.Err(err))
		return changed, err
	}

	if variables.Version == math.MaxUint64 {
		log.Error("variables.version not init, this is not checkpoint")
		return changed, errors.New("variables version not init")
	}

	if sm.systemVariables.Gid != 0 && variables.Gid != sm.systemVariables.Gid {
		log.Error("gid not same", log.Uint64("checkpoint.gid", variables.Gid), log.Uint64("now.gid", sm.systemVariables.Gid))
		return changed, errors.New("invalid gid")
	}

	if sm.systemVariables.Version != math.MaxUint64 &&
		variables.Version <= sm.systemVariables.Version {
		log.Info("lag checkpoint, no need update",
			log.Uint64("checkpoint.version", variables.Version),
			log.Uint64("now.version", sm.systemVariables.Version))
		return changed, nil
	}

	changed = true
	oldVariables := sm.systemVariables

	err = sm.UpdateSystemVariables(variables)
	if err != nil {
		return changed, err
	}

	log.Info("ok", log.Uint64("checkpoint.version", variables.Version),
		log.Int("checkpoint.member_count", len(variables.MemberShip)),
		log.Uint64("old.version", oldVariables.Version),
		log.Int("old.member_count", len(oldVariables.MemberShip)),
	)

	return changed, nil
}

func (sm *SystemVSM) GetSystemVariables() *SystemVariables {
	return sm.systemVariables
}

func (sm *SystemVSM) UpdateSystemVariables(variables *SystemVariables) error {
	options := NewWriteOptions()
	options.Sync = true

	err := sm.systemVStore.Write(options, sm.myGroupIdx, variables)
	if err != nil {

		return err
	}

	sm.systemVariables = variables
	sm.RefleshNodeID()
	return nil
}
