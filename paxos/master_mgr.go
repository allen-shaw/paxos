package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"math/rand"
	"time"
)

type MasterMgr struct {
	node            Node
	defaultMasterSM *MasterStateMachine

	leaseTime int
	isEnd     bool
	isStarted bool

	myGroupIdx     int
	needDropMaster bool
}

func NewMasterMgr(node Node, groupIdx int, logStorage LogStorage, callback MasterChangeCallback) *MasterMgr {
	mgr := &MasterMgr{}

	mgr.defaultMasterSM = NewMasterStateMachine(logStorage, node.GetMyNodeID(), groupIdx, callback)
	mgr.leaseTime = 10000
	mgr.node = node
	mgr.myGroupIdx = groupIdx

	mgr.isEnd = false
	mgr.isStarted = false

	mgr.needDropMaster = false

	return mgr
}

func (m *MasterMgr) Init() error {
	return m.defaultMasterSM.Init()
}

func (m *MasterMgr) SetLeaseTime(leaseTime int) {
	if leaseTime < 1000 {
		return
	}
	m.leaseTime = leaseTime
}

func (m *MasterMgr) DropMaster() {
	m.needDropMaster = true
}

func (m *MasterMgr) StopMaster() {
	if m.isStarted {
		m.isEnd = true
		m.join()
	}
}

func (m *MasterMgr) join() {

}

func (m *MasterMgr) RunMaster() {
	m.Start()
}

func (m *MasterMgr) Start() {
	go m.Run()
}

func (m *MasterMgr) Run() {
	m.isStarted = true
	for {
		if m.isEnd {
			return
		}

		leaseTime := m.leaseTime
		beginTime := GetCurrentTimeMs()

		m.TryMaster(leaseTime)

		continueLeaseTimeout := (leaseTime - 100) / 4
		continueLeaseTimeout = continueLeaseTimeout/2 + rand.Intn(continueLeaseTimeout)

		if m.needDropMaster {
			m.needDropMaster = false
			continueLeaseTimeout = leaseTime * 2
			log.Info("need drop master", log.Int("this round wait time ms", continueLeaseTimeout))
		}

		endTime := GetCurrentTimeMs()
		runTime := endTime - beginTime
		if runTime < 0 {
			runTime = 0
		}
		needSleepTime := uint64(continueLeaseTimeout) - runTime
		if needSleepTime < 0 {
			needSleepTime = 0
		}

		log.Info("try be master", log.Uint64("sleep time ms", needSleepTime))
		time.Sleep(time.Duration(needSleepTime) * time.Millisecond)
	}
}

func (m *MasterMgr) TryMaster(time int) {
	//step 1 check exist master and get version
	masterNodeId, masterVersion := m.defaultMasterSM.SafeGetMaster()

	if masterNodeId != nilNode && masterNodeId != m.node.GetMyNodeID() {
		log.Info("Ohter as master, can't try be master",
			log.Uint64("masterid", uint64(masterNodeId)),
			log.Uint64("myid", uint64(m.node.GetMyNodeID())))
		return
	}

	//step 2 try be master
	paxosValue, ok := MakeOpValue(m.node.GetMyNodeID(), masterVersion, m.leaseTime, MasterOperatorTypeComplete)
	if !ok {
		log.Error("make paxos value fail")
		return
	}

	masterLeaseTimeout := uint64(m.leaseTime - 100)
	absMasterTimeout := GetCurrentTimeMs() - masterLeaseTimeout

	smCtx := NewSMCtx(MasterVSMID, &absMasterTimeout)
	_, err := m.node.ProposeWithSMCtx(m.myGroupIdx, paxosValue, smCtx)
	if err != nil {
		log.Error("node propose fail", log.Err(err))
	}
}

func (m *MasterMgr) GetMasterSM() *MasterStateMachine {
	return m.defaultMasterSM
}
