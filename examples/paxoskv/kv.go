package paxoskv

import (
	"github.com/AllenShaw19/paxos/paxos"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/cespare/xxhash/v2"
)

type PaxosKV struct {
	node         *paxos.NodeInfo
	nodeList     paxos.NodeInfoList
	kvDBPath     string
	paxosLogPath string

	groupCount int
	paxosNode  paxos.Node
	kvSM       *PaxosKVSM
}

func NewPaxosKV(node *paxos.NodeInfo, nodeList paxos.NodeInfoList, kvDBPath string, paxosLogPath string) *PaxosKV {
	kv := &PaxosKV{node: node, nodeList: nodeList, kvDBPath: kvDBPath, paxosLogPath: paxosLogPath}
	kv.kvSM = NewPaxosKVSM(kvDBPath)
	kv.groupCount = 3
	return kv
}

func (kv *PaxosKV) RunPaxos() error {
	err := kv.kvSM.Init()
	if err != nil {
		return err
	}

	options := &paxos.Options{}
	options.LogStoragePath = kv.paxosLogPath

	//this groupcount means run paxos group count.
	//every paxos group is independent, there are no any communicate between any 2 paxos group.
	options.GroupCount = kv.groupCount

	options.MyNode = kv.node
	options.NodeInfoList = kv.nodeList

	//because all group share state machine(kv), so every group have same state machine.
	//just for split key to different paxos group, to upgrate performance.
	for groupIdx := 0; groupIdx < kv.groupCount; groupIdx++ {
		smInfo := paxos.GroupSMInfo{}
		smInfo.GroupIdx = groupIdx
		smInfo.SMList = append(smInfo.SMList, kv.kvSM)
		smInfo.IsUseMaster = true

		options.GroupSMInfos = append(options.GroupSMInfos, smInfo)
	}

	kv.paxosNode, err = paxos.RunNode(options)
	if err != nil {
		log.Error("run paxos fail", log.Err(err))
		return err
	}

	log.Info("run paxos ok")
	return nil
}

func (kv *PaxosKV) GetMaster(key string) *paxos.NodeInfo {
	groupIdx := kv.getGroupIdx(key)
	return kv.paxosNode.GetMaster(groupIdx)
}

func (kv *PaxosKV) IsMaster(key string) bool {
	groupIdx := kv.getGroupIdx(key)
	return kv.paxosNode.IsMaster(groupIdx)
}

func (kv *PaxosKV) Put(key, value string, version uint64) error {
	paxosValue, err := MakeSetOpValue(key, value, version)
	if err != nil {
		log.Error("make set opvalue fail", log.Err(err))
		return ErrKVStatusFail
	}

	kvSMCtx := NewPaxosKVSMCtx()
	err = kv.propose(key, paxosValue, kvSMCtx)
	if err != nil {
		log.Error("kv propose fail", log.Err(err))
		return ErrKVStatusFail
	}

	if kvSMCtx.ExecuteRet == nil {
		return nil
	}

	if kvSMCtx.ExecuteRet == ErrKvClientKeyVersionConflict {
		return ErrKVStatusVersionConflict
	}

	return ErrKVStatusFail
}

func (kv *PaxosKV) GetLocal(key string) (value string, version uint64, err error) {
	value, version, err = kv.kvSM.GetKVClient().Get(key)
	if err == nil {
		return value, version, nil
	}
	if err == ErrKvClientKeyNotExist {
		return "", 0, ErrKVStatusKeyNotExist
	}
	return "", 0, ErrKVStatusFail
}

func (kv *PaxosKV) Delete(key string, version uint64) error {
	paxosValue, err := MakeDelOpValue(key, version)
	if err != nil {
		log.Error("make del opvalue fail", log.Err(err))
		return ErrKVStatusFail
	}

	kvSMCtx := NewPaxosKVSMCtx()
	err = kv.propose(key, paxosValue, kvSMCtx)
	if err != nil {
		log.Error("kv propose fail", log.Err(err))
		return ErrKVStatusFail
	}

	if kvSMCtx.ExecuteRet == nil {
		return nil
	}

	if kvSMCtx.ExecuteRet == ErrKvClientKeyVersionConflict {
		return ErrKVStatusVersionConflict
	}

	return ErrKVStatusFail
}

func (kv *PaxosKV) getGroupIdx(key string) int {
	return int(xxhash.Sum64String(key) % uint64(kv.groupCount))
}

func (kv *PaxosKV) propose(key, paxosValue string, ctx *PaxosKVSMCtx) error {
	groupIdx := kv.getGroupIdx(key)

	smCtx := paxos.NewSMCtx(1, ctx)
	_, err := kv.paxosNode.ProposeWithSMCtx(groupIdx, paxosValue, smCtx)
	if err != nil {
		log.Error("paxos propose fail", log.String("key", key),
			log.Int("groupIdx", groupIdx), log.Err(err))
		return err
	}

	return nil
}
