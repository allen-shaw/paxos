package paxos

import (
	"github.com/AllenShaw19/paxos/plugin/log"
	"time"
)

type Replayer struct {
	config        *Config
	smFac         *SMFac
	paxosLog      *PaxosLog
	checkpointMgr *CheckpointMgr

	canRun   bool
	isPaused bool
	isEnd    bool
}

func NewReplayer(config *Config,
	smFac *SMFac,
	logStorage LogStorage,
	checkpointMgr *CheckpointMgr) *Replayer {
	r := &Replayer{}

	r.config = config
	r.smFac = smFac
	r.paxosLog = NewPaxosLog(logStorage)
	r.checkpointMgr = checkpointMgr
	r.canRun = false
	r.isPaused = true
	r.isEnd = false

	return r
}

func (r *Replayer) Start() {
	go r.Run()
}

func (r *Replayer) Stop() {
	r.isEnd = true
}

func (r *Replayer) Run() {
	log.Info("checkpoint.replayer [START]")
	instanceID := r.smFac.GetCheckpointInstanceID(r.config.GetMyGroupIdx()) + 1

	for {
		if r.isEnd {
			log.Info("checkpoint.replayer [END]")
			return
		}
		if !r.canRun {
			log.Info("pausing")
			r.isPaused = true
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		if instanceID >= r.checkpointMgr.GetMaxChosenInstanceID() {
			log.Info("now max chosen instance_id small than execute instance_id, wait",
				log.Uint64("max_chosen_instance_id", r.checkpointMgr.GetMaxChosenInstanceID()),
				log.Uint64("execute_instance_id", instanceID))
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		ok := r.playOne(instanceID)
		if ok {
			log.Info("play one done", log.Uint64("instance_id", instanceID))
			instanceID++
		} else {
			log.Error("play one fail", log.Uint64("instance_id", instanceID))
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (r *Replayer) Pause() {
	r.canRun = false
}

func (r *Replayer) Continue() {
	r.isPaused = false
	r.canRun = true
}

func (r *Replayer) IsPaused() bool {
	return r.isPaused
}

func (r *Replayer) playOne(instanceID uint64) bool {
	groupIdx := r.config.GetMyGroupIdx()
	state, err := r.paxosLog.ReadState(groupIdx, instanceID)
	if err != nil {
		log.Error("paxoslog readstate fail", log.Uint64("instance_id", instanceID), log.Err(err))
		return false
	}

	ok := r.smFac.ExecuteForCheckpoint(groupIdx, instanceID, string(state.AcceptedValue))
	if !ok {
		log.Error("checkpoint sm execute fail", log.Uint64("instance_id", instanceID))
	}

	return ok
}
