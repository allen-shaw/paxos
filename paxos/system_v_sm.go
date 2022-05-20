package paxos

import "github.com/AllenShaw19/paxos/log"

type SystemVSM struct {
	myGroupIdx      int
	systemVariables *SystemVariables
	systemVStore    *SystemVariablesStore

	nodeIDs  map[NodeID]struct{}
	myNodeID NodeID

	membershipChangeCallback MembershipChangeCallback
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
		sm.systemVariables.Version = -1
		log.Info("variables not exists")
	} else {
		sm.RefleshNodeID()
		log.Info("init succ.", log.Int("groupidx", sm.myGroupIdx),
			log.Uint64("gid", sm.systemVariables.Gid),
			log.Uint64("version", sm.systemVariables.Version))
	}
	return nil
}

func (sm *SystemVSM) UpdateSystemVariables(variables *SystemVariables) error {
	options := NewWriteOptions()
	options.Sync = true

	err := sm.systemVStore.Write(options, sm.myGroupIdx, variables)
	if err != nil {

		return err
	}

	sm.systemVariables = variables

}

func (sm *SystemVSM) Execute(groupIdx int, instanceID uint64, value string, ctx *SMCtx) {

}

func (sm *SystemVSM) SMID() int {

}

func (sm *SystemVSM) GetGid() uint64 {

}

//membership

func (sm *SystemVSM) AddNodeIDList(nodeInfos NodeInfoList) {

}

func (sm *SystemVSM) RefleshNodeID() {

}

func (sm *SystemVSM) GetMembershipMap() map[NodeID]struct{} {
	return sm.nodeIDs
}
