package paxos

import (
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/golang/protobuf/proto"
)

type PaxosLog struct {
	logStorage LogStorage
}

func NewPaxosLog(logStorage LogStorage) *PaxosLog {
	return &PaxosLog{logStorage: logStorage}
}

func (l *PaxosLog) WriteLog(options *WriteOptions, groupIdx int, instanceID uint64, value string) error {
	state := &AcceptorStateData{
		InstanceID:     instanceID,
		AcceptedValue:  []byte(value),
		PromiseID:      0,
		PromiseNodeID:  uint64(nilNode),
		AcceptedID:     0,
		AcceptedNodeID: uint64(nilNode),
	}

	err := l.WriteState(options, groupIdx, instanceID, state)
	if err != nil {
		log.Error("write state to db fail", log.Int("group_idx", groupIdx),
			log.Uint64("instance_id", instanceID),
			log.Err(err))
		return err
	}

	log.Info("write state succ", log.Int("group_idx", groupIdx),
		log.Uint64("instance_id", instanceID),
		log.Int("value_size", len(value)))
	return nil
}

func (l *PaxosLog) ReadLog(groupIdx int, instanceID uint64) (string, error) {
	state, err := l.ReadState(groupIdx, instanceID)
	if err != nil {
		log.Error("read state from db fail", log.Int("group_idx", groupIdx),
			log.Uint64("instance_id", instanceID),
			log.Err(err))
		return "", err
	}

	value := state.AcceptedValue

	log.Info("read state succ", log.Int("group_idx", groupIdx),
		log.Uint64("instance_id", instanceID),
		log.Int("value_size", len(value)))
	return string(value), nil
}

func (l *PaxosLog) GetMaxInstanceIDFromLog(groupIdx int) (uint64, error) {
	instanceID, err := l.logStorage.GetMaxInstanceID(groupIdx)
	if err != nil && err != ErrNotExist {
		log.Error("log storage get max instance_id fail", log.Int("group_idx", groupIdx), log.Err(err))
		return 0, err
	}
	if err == ErrNotExist {
		log.Error("max instance_id not found", log.Int("group_idx", groupIdx))
		return 0, ErrNotExist
	}

	log.Info("get max instance_id succ", log.Uint64("instance_id", instanceID), log.Int("group_idx", groupIdx))
	return instanceID, nil
}

func (l *PaxosLog) WriteState(options *WriteOptions, groupIdx int, instanceID uint64, state *AcceptorStateData) error {
	buff, err := proto.Marshal(state)
	if err != nil {
		log.Error("state marshal fail", log.Err(err))
		return err
	}

	err = l.logStorage.Put(options, groupIdx, instanceID, string(buff))
	if err != nil {
		log.Error("log storage set fail", log.Int("group_idx", groupIdx), log.Int("buffer size", len(buff)), log.Err(err))
		return err
	}

	return nil
}

func (l *PaxosLog) ReadState(groupIdx int, instanceID uint64) (*AcceptorStateData, error) {
	buff, err := l.logStorage.Get(groupIdx, instanceID)
	if err != nil && err != ErrNotExist {
		log.Error("log storage get fail", log.Int("group_idx", groupIdx), log.Err(err))
		return nil, err
	}
	if err == ErrNotExist {
		log.Error("log storage not found", log.Int("group_idx", groupIdx))
		return nil, ErrNotExist
	}

	state := &AcceptorStateData{}
	err = proto.Unmarshal([]byte(buff), state)
	if err != nil {
		log.Error("buff proto unmarshal fail", log.Err(err))
		return nil, err
	}

	return state, nil
}
