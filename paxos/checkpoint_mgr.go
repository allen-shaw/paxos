package paxos

import (
	"github.com/AllenShaw19/paxos/plugin/log"
)

type CheckpointMgr struct {
	config     *Config
	logStorage LogStorage
	smFac      *SMFac

	replayer *Replayer
	cleaner  *Cleaner

	minChosenInstanceID uint64
	maxChosenInstanceID uint64

	inAskForCheckpointMode   bool
	needAsk                  map[NodeID]struct{}
	lastAckForCheckpointTime uint64

	useCheckpointReplayer bool
}

func NewCheckpointMgr(config *Config,
	smFac *SMFac,
	logStorage LogStorage,
	useCheckpointReplayer bool) *CheckpointMgr {
	m := &CheckpointMgr{}

	m.config = config
	m.logStorage = logStorage
	m.smFac = smFac
	m.replayer = NewReplayer(config, smFac, logStorage, m)
	m.cleaner = NewCleaner(config, smFac, logStorage, m)
	m.maxChosenInstanceID = 0
	m.minChosenInstanceID = 0
	m.inAskForCheckpointMode = false
	m.useCheckpointReplayer = useCheckpointReplayer
	m.lastAckForCheckpointTime = 0

	return m
}

func (m *CheckpointMgr) Init() error {
	groupIdx := m.config.GetMyGroupIdx()
	minChosenInstanceID, err := m.logStorage.GetMinChosenInstanceID(groupIdx)
	if err != nil {
		return err
	}
	err = m.cleaner.FixMinChosenInstanceID(minChosenInstanceID)
	if err != nil {
		return err
	}
	return nil
}

func (m *CheckpointMgr) Start() {
	if m.useCheckpointReplayer {
		m.replayer.Start()
	}
	m.cleaner.Start()
}

func (m *CheckpointMgr) Stop() {
	if m.useCheckpointReplayer {
		m.replayer.Stop()
	}
	m.cleaner.Stop()
}

func (m *CheckpointMgr) GetReplayer() *Replayer {
	return m.replayer
}

func (m *CheckpointMgr) GetCleaner() *Cleaner {
	return m.cleaner
}

func (m *CheckpointMgr) PrepareForAskForCheckpoint(sendNodeID NodeID) error {
	if _, ok := m.needAsk[sendNodeID]; !ok {
		m.needAsk[sendNodeID] = struct{}{}
	}

	if m.lastAckForCheckpointTime == 0 {
		m.lastAckForCheckpointTime = GetCurrentTimeMs()
	}

	nowTime := GetCurrentTimeMs()
	if nowTime > m.lastAckForCheckpointTime+60000 {
		log.Info("no majority reply, just ask for checkpoint")
	} else {
		if len(m.needAsk) < m.config.GetMajorityCount() {
			log.Info("need more other tell us need to ask for checkpoint")
			return ErrNotMajority
		}
	}

	m.lastAckForCheckpointTime = 0
	m.inAskForCheckpointMode = true
	return nil
}

func (m *CheckpointMgr) InAskForCheckpointMode() bool {
	return m.inAskForCheckpointMode
}

func (m *CheckpointMgr) ExitCheckpointMode() {
	m.inAskForCheckpointMode = false
}

func (m *CheckpointMgr) GetMinChosenInstanceID() uint64 {
	return m.minChosenInstanceID
}

func (m *CheckpointMgr) SetMinChosenInstanceIDCache(instanceID uint64) {
	m.minChosenInstanceID = instanceID
}

func (m *CheckpointMgr) SetMinChosenInstanceID(instanceID uint64) error {
	options := &WriteOptions{Sync: true}
	err := m.logStorage.SetMinChosenInstanceID(options, m.config.GetMyGroupIdx(), instanceID)
	if err != nil {
		return err
	}
	m.minChosenInstanceID = instanceID
	return nil
}

func (m *CheckpointMgr) GetCheckpointInstanceID() uint64 {
	return m.smFac.GetCheckpointInstanceID(m.config.GetMyGroupIdx())
}

func (m *CheckpointMgr) GetMaxChosenInstanceID() uint64 {
	return m.maxChosenInstanceID
}

func (m *CheckpointMgr) SetMaxChosenInstanceID(instanceID uint64) {
	m.maxChosenInstanceID = instanceID
}
