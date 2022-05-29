package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
	"math"
	"sync"
)

type MasterOperatorType int

const MasterOperatorTypeComplete MasterOperatorType = 1

type MasterStateMachine struct {
	myGroupIdx int
	myNodeID   NodeID

	masterVStore *MasterVariablesStore

	masterNodeID  NodeID
	masterVersion uint64
	leaseTime     uint32
	absExpireTime uint64

	mutex sync.Mutex

	masterChangeCallback MasterChangeCallback
}

func NewMasterStateMachine(logStorage LogStorage,
	myNodeID NodeID,
	groupIdx int,
	masterChangeCallback MasterChangeCallback) *MasterStateMachine {
	sm := &MasterStateMachine{}

	sm.masterVStore = NewMasterVariablesStore(logStorage)
	sm.masterChangeCallback = masterChangeCallback
	sm.myGroupIdx = groupIdx
	sm.myNodeID = myNodeID

	sm.masterNodeID = nilNode
	sm.masterVersion = math.MaxUint64
	sm.leaseTime = 0
	sm.absExpireTime = 0

	return nil
}

func (sm *MasterStateMachine) Execute(groupIdx int, instanceID uint64, value string, smCtx *SMCtx) bool {

}

func (sm *MasterStateMachine) SMID() int {
	return MasterVSMID
}

func (sm *MasterStateMachine) ExecuteForCheckpoint(groupIdx int,
	instanceID uint64, paxosValue string) bool {
	return true
}

func (sm *MasterStateMachine) GetCheckpointInstanceID(groupIdx int) uint64 {
	return sm.masterVersion
}

func (sm *MasterStateMachine) BeforePropose(groupIdx int, value string) {

}

func (sm *MasterStateMachine) NeedCallBeforePropose() bool {

}

////////////////////////////////////////////////////////

func (sm *MasterStateMachine) GetCheckpointState(groupIdx int) (dirPath string, files []string, err error) {
	return "", nil, nil
}

func (sm *MasterStateMachine) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, files []string, checkpointInstanceID uint64) error {
	return nil
}

func (sm *MasterStateMachine) LockCheckpointState() error {
	return nil
}

func (sm *MasterStateMachine) UnLockCheckpointState() {}

/////////////////////////////////////////////////////////

func (sm *MasterStateMachine) Init() error {

}

func (sm *MasterStateMachine) LearnMaster(instanceID uint64, masterOper *MasterOperator, absMasterTimeout uint64) error {

}

func (sm *MasterStateMachine) GetMaster() NodeID {

}

func (sm *MasterStateMachine) GetMasterWithVersion(version uint64) NodeID {

}

func (sm *MasterStateMachine) IsIMMaster() bool {

}

/////////////////////////////////////////////////////////

func (sm *MasterStateMachine) UpdateMasterToStore(masterNodeID NodeID,
	version uint64, leaseTime uint32) error {

}

func (sm *MasterStateMachine) SafeGetMaster() (masterNodeID NodeID, masterVersion uint64) {

}

/////////////////////////////////////////////////////////

func (sm *MasterStateMachine) GetCheckpointBuffer() (string, error) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.masterVersion == math.MaxUint64 {
		return "", nil
	}

	variables := &MasterVariables{}
	variables.MasterNodeId = uint64(sm.masterNodeID)
	variables.Version = sm.masterVersion
	variables.LeaseTime = sm.leaseTime

	buff, err := proto.Marshal(variables)
	if err != nil {
		log.Error("master variables marshal fail", log.Err(err))
		return "", err
	}

	return string(buff), err
}

func (sm *MasterStateMachine) UpdateByCheckpoint(buff []byte) (changed bool, err error) {

}

func MakeOpValue(nodeID NodeID,
	version uint64,
	timeout int,
	op MasterOperatorType) (string, bool) {

}
