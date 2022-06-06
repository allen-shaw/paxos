package paxoskv

import "github.com/AllenShaw19/paxos/paxos"

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
		return err
	}

	return nil
}
