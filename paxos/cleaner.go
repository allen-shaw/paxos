package paxos

import (
	"github.com/AllenShaw19/paxos/plugin/log"
	"math/rand"
	"time"
)

const (
	kCanDeleteDelta     = 1000000
	kDeleteSaveInterval = 100
)

type Cleaner struct {
	config        *Config
	smFac         *SMFac
	logStorage    LogStorage
	checkpointMgr *CheckpointMgr

	lastSave uint64

	canRun   bool
	isPaused bool
	isEnd    bool
	isStart  bool

	holdCount uint64
}

func NewCleaner(config *Config,
	smFac *SMFac,
	logStorage LogStorage,
	checkpointMgr *CheckpointMgr) *Cleaner {
	c := &Cleaner{
		config:        config,
		smFac:         smFac,
		logStorage:    logStorage,
		checkpointMgr: checkpointMgr,
		lastSave:      0,
		canRun:        false,
		isPaused:      true,
		isEnd:         false,
		isStart:       false,
		holdCount:     kCanDeleteDelta,
	}

	return c
}

func (c *Cleaner) Start() {
	go c.Run()
}

func (c *Cleaner) Stop() {
	c.isEnd = true
}

func (c *Cleaner) Pause() {
	c.canRun = false
}

func (c *Cleaner) Continue() {
	c.isPaused = false
	c.canRun = true
}

func (c *Cleaner) IsPaused() bool {
	return c.isPaused
}

func (c *Cleaner) Run() {
	c.isStart = true
	c.Continue()

	groupIdx := c.config.GetMyGroupIdx()

	//control delete speed to avoid affecting the io too much.
	deleteQps := CleanerDeleteQps()
	sleepMs := 1000 / deleteQps
	if deleteQps > 1000 {
		sleepMs = 1
	}
	deleteInterval := 1
	if deleteQps > 1000 {
		deleteInterval = deleteQps / 1000
	}

	log.Debug("cleaner run", log.Int("delete_qps", deleteQps),
		log.Int("sleep_ms", sleepMs),
		log.Int("delete_interval", deleteInterval))

	for {
		if c.isEnd {
			log.Info("checkpoint.cleaner [END]")
			return
		}

		if !c.canRun {
			log.Info("pausing, wait")
			c.isPaused = true
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		instanceID := c.checkpointMgr.GetMinChosenInstanceID()
		cpInstanceID := c.smFac.GetCheckpointInstanceID(groupIdx)
		maxChosenInstanceID := c.checkpointMgr.GetMaxChosenInstanceID()

		deleteCount := 0
		for (instanceID+c.holdCount < cpInstanceID) &&
			(instanceID+c.holdCount < maxChosenInstanceID) {
			ok := c.deleteOne(instanceID)
			if ok {
				instanceID++
				deleteCount++
				if deleteCount >= deleteInterval {
					deleteCount = 0
					time.Sleep(time.Duration(sleepMs) * time.Millisecond)
				}
			} else {
				log.Debug("delete instance fail", log.Uint64("instance_id", instanceID))
				break
			}
		}

		if cpInstanceID == 0 {
			log.Info("sleep a while", log.Uint64("max_deleted_instance_id", instanceID),
				log.String("checkpoint_instance_id", "no checkpoint"),
				log.Uint64("now_instance_id", c.checkpointMgr.GetMaxChosenInstanceID()))
		} else {
			log.Info("sleep a while", log.Uint64("max_deleted_instance_id", instanceID),
				log.Uint64("checkpoint_instance_id", cpInstanceID),
				log.Uint64("now_instance_id", c.checkpointMgr.GetMaxChosenInstanceID()))
		}
		time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
	}
}

func (c *Cleaner) deleteOne(instanceID uint64) bool {
	groupIdx := c.config.GetMyGroupIdx()
	options := &WriteOptions{Sync: false}

	err := c.logStorage.Del(options, groupIdx, instanceID)
	if err != nil {
		return false
	}

	c.checkpointMgr.SetMinChosenInstanceIDCache(instanceID)

	if instanceID >= c.lastSave+kDeleteSaveInterval {
		err := c.checkpointMgr.SetMinChosenInstanceID(instanceID + 1)
		if err != nil {
			log.Error("set min chosen instance_id fail", log.Uint64("now_delete_instance_id", instanceID), log.Err(err))
			return false
		}

		c.lastSave = instanceID
		log.Info("delete instance done", log.Uint64("deleted_instance_id", kDeleteSaveInterval),
			log.Uint64("now_min_chosen_instance_id", instanceID+1))
	}

	return true
}

func (c *Cleaner) SetHoldPaxosLogCount(holdCount uint64) {
	if holdCount < 300 {
		c.holdCount = 300
	} else {
		c.holdCount = holdCount
	}
}

func (c *Cleaner) FixMinChosenInstanceID(oldMinChosenInstanceID uint64) error {
	groupIdx := c.config.GetMyGroupIdx()
	cpInstanceID := c.smFac.GetCheckpointInstanceID(groupIdx) + 1
	fixMinChosenInstanceID := oldMinChosenInstanceID

	for instanceID := oldMinChosenInstanceID; instanceID < oldMinChosenInstanceID+kDeleteSaveInterval; instanceID++ {
		if instanceID >= cpInstanceID {
			break
		}

		_, err := c.logStorage.Get(groupIdx, instanceID)
		if err != nil && err != ErrNotExist {
			return err
		} else if err == ErrNotExist {
			fixMinChosenInstanceID = instanceID + 1
		} else {
			break
		}
	}

	if fixMinChosenInstanceID > oldMinChosenInstanceID {
		err := c.checkpointMgr.SetMinChosenInstanceID(fixMinChosenInstanceID)
		if err != nil {
			return err
		}
	}

	log.Info("fix min chosen instance id success", log.Uint64("old_min_chosen", oldMinChosenInstanceID),
		log.Uint64("fix_min_chosen", fixMinChosenInstanceID))
	return nil
}
