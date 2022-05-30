package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
	"math"
	"math/rand"
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
	masterOper := &MasterOperator{}
	err := proto.Unmarshal([]byte(value), masterOper)
	if err != nil {
		log.Error("master oper data fail", log.Err(err))
		return false
	}

	if MasterOperatorType(masterOper.Operator) == MasterOperatorTypeComplete {
		var pAbsmastertimeout *uint64
		if smCtx != nil && smCtx.Ctx != nil {
			pAbsmastertimeout = smCtx.Ctx.(*uint64)
		}

		var absMasterTimeout uint64
		if pAbsmastertimeout != nil {
			absMasterTimeout = *pAbsmastertimeout
		}

		log.Info("complete", log.Uint64("abs_master_timeout", absMasterTimeout))
		err := sm.LearnMaster(instanceID, masterOper, absMasterTimeout)
		if err != nil {
			return false
		}

	} else {
		log.Error("unknown op", log.Uint32("op", masterOper.Operator))
		//wrong op, just skip, so return true;
		return true
	}

	return true
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

func (sm *MasterStateMachine) BeforePropose(groupIdx int, value string) (string, error) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	masterOper := &MasterOperator{}
	err := proto.Unmarshal([]byte(value), masterOper)
	if err != nil {
		return "", err
	}

	masterOper.Lastversion = sm.masterVersion
	buff, err := proto.Marshal(masterOper)
	if err != nil {
		return "", err
	}
	return string(buff), nil
}

func (sm *MasterStateMachine) NeedCallBeforePropose() bool {
	return true
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
	variables, err := sm.masterVStore.Read(sm.myGroupIdx)
	if err != nil && err != ErrNotExist {
		log.Error("master variables read from store fail", log.Err(err))
		return err
	}

	if err == ErrNotExist {
		log.Info("no master variables exist")
	} else {
		sm.masterVersion = variables.Version
		if NodeID(variables.MasterNodeId) == sm.myNodeID {
			sm.masterNodeID = nilNode
			sm.absExpireTime = 0
		} else {
			sm.masterNodeID = NodeID(variables.MasterNodeId)
			sm.absExpireTime = GetCurrentTimeMs() + uint64(variables.LeaseTime)
		}
	}

	log.Info("OK", log.Uint64("master.nodeid", uint64(sm.masterNodeID)),
		log.Uint64("version", sm.masterVersion),
		log.Uint64("expired_time", sm.absExpireTime))
	return nil
}

func (sm *MasterStateMachine) LearnMaster(instanceID uint64, masterOper *MasterOperator, absMasterTimeout uint64) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	log.Debug("learn master",
		log.Uint64("my_last_version", sm.masterVersion),
		log.Uint64("other_last_version", masterOper.Lastversion),
		log.Uint64("this_version", masterOper.Version),
		log.Uint64("instance_id", instanceID))

	if masterOper.Lastversion != 0 &&
		instanceID > sm.masterVersion &&
		masterOper.Lastversion != sm.masterVersion {
		log.Error("other last version not same as my last version",
			log.Uint64("other_last_version", masterOper.Lastversion),
			log.Uint64("my_last_version", sm.masterVersion),
			log.Uint64("instance_id", instanceID))
		log.Info("try to fix, set_my_master_version as other last version",
			log.Uint64("my_master_version", sm.masterVersion),
			log.Uint64("other_last_version", masterOper.Lastversion),
			log.Uint64("instance_id", instanceID))
		sm.masterVersion = masterOper.Lastversion
	}

	if masterOper.Version != sm.masterVersion {
		log.Info("version conflict", log.Uint64("op_version", masterOper.Version),
			log.Uint64("now_master_version", sm.masterVersion))
		return nil
	}

	err := sm.UpdateMasterToStore(NodeID(masterOper.Nodeid), instanceID, uint32(masterOper.Timeout))
	if err != nil {
		log.Error("update master to store fail", log.Err(err))
		return err
	}

	masterChange := false
	if sm.masterNodeID != NodeID(masterOper.Nodeid) {
		masterChange = true
	}

	sm.masterNodeID = NodeID(masterOper.Nodeid)
	if sm.masterNodeID == sm.myNodeID {
		//self be master
		//use local abstimeout
		sm.absExpireTime = absMasterTimeout
		log.Info("be master success", log.Uint64("abs_expire_time", sm.absExpireTime))
	} else {
		//other be master
		//use new start timeout
		sm.absExpireTime = GetCurrentTimeMs() + uint64(masterOper.Timeout)
		log.Info("other be master", log.Uint64("abs_expire_time", sm.absExpireTime))
	}

	sm.leaseTime = uint32(masterOper.Timeout)
	sm.masterVersion = instanceID

	if masterChange {
		if sm.masterChangeCallback != nil {
			sm.masterChangeCallback(sm.myGroupIdx, NewNodeInfo(sm.masterNodeID), sm.masterVersion)
		}
	}

	log.Info("OK", log.Uint64("master_nodeid", uint64(sm.masterNodeID)),
		log.Uint64("version", sm.masterVersion),
		log.Uint64("abs_timeout", sm.absExpireTime))

	return nil
}

func (sm *MasterStateMachine) GetMaster() NodeID {
	if GetCurrentTimeMs() >= sm.absExpireTime {
		return nilNode
	}
	return sm.masterNodeID
}

func (sm *MasterStateMachine) GetMasterWithVersion() (NodeID, uint64) {
	return sm.SafeGetMaster()
}

func (sm *MasterStateMachine) IsIMMaster() bool {
	masterNodeID := sm.GetMaster()
	return masterNodeID == sm.myNodeID
}

/////////////////////////////////////////////////////////

func (sm *MasterStateMachine) UpdateMasterToStore(masterNodeID NodeID,
	version uint64, leaseTime uint32) error {
	variables := &MasterVariables{}
	variables.MasterNodeId = uint64(masterNodeID)
	variables.Version = version
	variables.LeaseTime = leaseTime

	options := &WriteOptions{Sync: true}

	return sm.masterVStore.Write(options, sm.myGroupIdx, variables)
}

func (sm *MasterStateMachine) SafeGetMaster() (masterNodeID NodeID, masterVersion uint64) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if GetCurrentTimeMs() >= sm.absExpireTime {
		masterNodeID = nilNode
	} else {
		masterNodeID = sm.masterNodeID
	}
	masterVersion = sm.masterVersion
	return masterNodeID, masterVersion
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

func (sm *MasterStateMachine) UpdateByCheckpoint(checkpointBuffer []byte) (changed bool, err error) {
	if len(checkpointBuffer) == 0 {
		return false, nil
	}

	variables := &MasterVariables{}
	err = proto.Unmarshal(checkpointBuffer, variables)
	if err != nil {
		log.Error("unmarshal variables fail", log.Err(err))
		return false, err
	}

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if variables.Version <= sm.masterVersion && sm.masterVersion != math.MaxUint64 {
		log.Info("lag checkpoint, no need to update",
			log.Uint64("co.version", variables.Version),
			log.Uint64("now.version", sm.masterVersion))
		return false, nil
	}

	err = sm.UpdateMasterToStore(NodeID(variables.MasterNodeId), variables.Version, variables.LeaseTime)
	if err != nil {
		return false, err
	}

	log.Info("ok", log.Uint64("checkpoint.version", variables.Version),
		log.Uint64("checkpoint.master_nodeid", variables.MasterNodeId),
		log.Uint64("old.version", sm.masterVersion),
		log.Uint64("old.master_nodeid", uint64(sm.masterNodeID)))

	masterChanged := false
	sm.masterVersion = variables.Version

	if NodeID(variables.MasterNodeId) == sm.myNodeID {
		sm.masterNodeID = nilNode
		sm.absExpireTime = 0
	} else {
		if sm.masterNodeID != NodeID(variables.MasterNodeId) {
			masterChanged = true
		}
		sm.masterNodeID = NodeID(variables.MasterNodeId)
		sm.absExpireTime = GetCurrentTimeMs() + uint64(variables.LeaseTime)
	}

	if masterChanged {
		if sm.masterChangeCallback != nil {
			sm.masterChangeCallback(sm.myGroupIdx, NewNodeInfo(sm.masterNodeID), sm.masterVersion)
		}
	}

	return changed, nil
}

func MakeOpValue(nodeID NodeID,
	version uint64,
	timeout int,
	op MasterOperatorType) (string, bool) {
	masterOper := &MasterOperator{}
	masterOper.Nodeid = uint64(nodeID)
	masterOper.Version = version
	masterOper.Timeout = int32(timeout)
	masterOper.Operator = uint32(op)
	masterOper.Sid = rand.Uint32()

	buff, err := proto.Marshal(masterOper)
	if err != nil {
		return "", false
	}
	return string(buff), true
}
