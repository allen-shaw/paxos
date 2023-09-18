package paxoskv

import (
	pb "github.com/AllenShaw19/paxos/examples/paxoskv/proto"
	"github.com/AllenShaw19/paxos/log"
	"github.com/AllenShaw19/paxos/paxos"
	"github.com/golang/protobuf/proto"
	"math/rand"
)

type PaxosKVSMCtx struct {
	ExecuteRet  error
	readValue   string
	readVersion uint64
}

func NewPaxosKVSMCtx() *PaxosKVSMCtx {
	return &PaxosKVSMCtx{ExecuteRet: ErrUndefined, readVersion: 0}
}

type PaxosKVSM struct {
	dbPath                  string
	kvClient                *KVClient
	checkpointInstanceID    uint64
	skipSyncCheckpointTimes int
}

func NewPaxosKVSM(dbPath string) *PaxosKVSM {
	return &PaxosKVSM{
		dbPath:               dbPath,
		kvClient:             NewKVClient(),
		checkpointInstanceID: paxos.NoCheckpoint,
	}
}

func (sm *PaxosKVSM) Init() error {
	err := sm.kvClient.Init(sm.dbPath)
	if err != nil {
		log.Error("kv client init fail", log.Err(err), log.String("path", sm.dbPath))
		return err
	}

	sm.checkpointInstanceID, err = sm.kvClient.GetCheckpointInstanceID()
	if err != nil && err != ErrKvClientKeyNotExist {
		log.Error("kv client get checkpoint instanceid fail", log.Err(err))
		return err
	}

	if err == ErrKvClientKeyNotExist {
		log.Error("no checkpoint")
		sm.checkpointInstanceID = paxos.NoCheckpoint
		return nil
	}

	log.Info("checkpoint instanceid", log.Uint64("checkpoint.instanceid", sm.checkpointInstanceID))
	return nil
}

func (sm *PaxosKVSM) Execute(groupIdx int, instanceID uint64, paxosValue string, smCtx *paxos.SMCtx) bool {
	kvOper := &pb.KVOperator{}
	err := proto.Unmarshal([]byte(paxosValue), kvOper)
	if err != nil {
		log.Error("kvoper marshal fail, skip", log.Err(err))
		// wrong oper data, just skip, so return nil
		return true
	}

	var (
		readValue   string
		readVersion uint64
	)
	switch kvOper.Operator {
	case KVOperatorTypeRead:
		readValue, readVersion, err = sm.kvClient.Get(kvOper.Key)
	case KVOperatorTypeWrite:
		err = sm.kvClient.Set(kvOper.Key, string(kvOper.Value), kvOper.Version)
	case KVOperatorTypeDelete:
		err = sm.kvClient.Del(kvOper.Key, kvOper.Version)
	default:
		log.Error("unknown op", log.Uint32("oper", kvOper.Operator))
		//wrong op, just skip, so return true;
		return true
	}

	if err == ErrKvClientSysFail {
		//need retry
		return false
	}

	if smCtx != nil && smCtx.Ctx != nil {
		paxosKVSMCtx := smCtx.Ctx.(*PaxosKVSMCtx)
		paxosKVSMCtx.ExecuteRet = err
		paxosKVSMCtx.readValue = readValue
		paxosKVSMCtx.readVersion = readVersion
	}
	_ = sm.SyncCheckpointInstanceID(instanceID)
	return true
}

func (sm *PaxosKVSM) SMID() int {
	return 1
}

// ExecuteForCheckpoint no use
func (sm *PaxosKVSM) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue string) bool {
	return true
}

func (sm *PaxosKVSM) GetCheckpointInstanceID(groupIdx int) uint64 {
	return sm.checkpointInstanceID
}

// GetCheckpointState have checkpoint, but not impl auto copy checkpoint to other node, so return fail.
func (sm *PaxosKVSM) GetCheckpointState(groupIdx int) (dirPath string, files []string, err error) {
	return "", nil, nil
}

func (sm *PaxosKVSM) LockCheckpointState() error {
	return nil
}

func (sm *PaxosKVSM) UnLockCheckpointState() {}

func (sm *PaxosKVSM) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, files []string, checkpointInstanceID uint64) error {
	return nil
}

func (sm *PaxosKVSM) BeforePropose(groupIdx int, value string) (string, error) {
	return value, nil
}

func (sm *PaxosKVSM) NeedCallBeforePropose() bool {
	return false
}

///////////////////////////////////////////////////////

func (sm *PaxosKVSM) SyncCheckpointInstanceID(instanceID uint64) error {
	sm.skipSyncCheckpointTimes++
	if sm.skipSyncCheckpointTimes < 5 {
		log.Info("no need to sync checkpoint", log.Int("skip.times", sm.skipSyncCheckpointTimes))
		return nil
	}

	err := sm.kvClient.SetCheckpointInstanceID(instanceID)
	if err != nil {
		log.Error("kvclient.SetCheckpointInstanceID fail", log.Err(err), log.Uint64("instanceid", instanceID))
		return err
	}

	log.Info("ok", log.Uint64("old.checkpoint.instanceid", sm.checkpointInstanceID),
		log.Uint64("new.checkpoint.instanceid", instanceID))

	sm.checkpointInstanceID = instanceID
	sm.skipSyncCheckpointTimes = 0

	return nil
}

func (sm *PaxosKVSM) GetKVClient() *KVClient {
	return sm.kvClient
}

///////////////////////////////////////////////////////

func MakeOpValue(key, value string, version uint64, op uint32) (string, error) {
	oper := &pb.KVOperator{
		Key:      key,
		Value:    []byte(value),
		Version:  version,
		Operator: op,
		Sid:      rand.Uint32(),
	}
	paxosValue, err := proto.Marshal(oper)
	return string(paxosValue), err
}

func MakeGetOpValue(key string) (string, error) {
	return MakeOpValue(key, "", 0, KVOperatorTypeRead)
}

func MakeSetOpValue(key, value string, version uint64) (string, error) {
	return MakeOpValue(key, value, version, KVOperatorTypeWrite)
}

func MakeDelOpValue(key string, version uint64) (string, error) {
	return MakeOpValue(key, "", version, KVOperatorTypeDelete)
}
