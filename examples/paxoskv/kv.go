package paxoskv

import (
	"github.com/AllenShaw19/paxos/paxos"
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

}

func (kv *PaxosKV) GetLocal(key string) (value string, version uint64, err error) {

}

func (kv *PaxosKV) Delete(key string, version uint64) error {

}

func (kv *PaxosKV) getGroupIdx(key string) int {

}

func (kv *PaxosKV) kvPropose(key, paxosValue string, ctx *PaxosKVSMCtx) {

}
