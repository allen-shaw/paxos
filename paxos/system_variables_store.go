package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
)

type SystemVariablesStore struct {
	logStorage LogStorage
}

func NewSystemVariablesStore(logStorage LogStorage) *SystemVariablesStore {
	return &SystemVariablesStore{logStorage: logStorage}
}

func (s *SystemVariablesStore) Write(options *WriteOptions, groupIdx int, variables *SystemVariables) error {
	buff, err := proto.Marshal(variables)
	if err != nil {
		log.Error("variables marshal fail", log.Err(err))
		return err
	}

	err = s.logStorage.SetSystemVariables(options, groupIdx, string(buff))
	if err != nil {
		log.Error("log storage set fail", log.Int("group_idx", groupIdx), log.Int("buffer size", len(buff)), log.Err(err))
		return err
	}

	return nil
}

func (s *SystemVariablesStore) Read(groupIdx int) (*SystemVariables, error) {
	buff, err := s.logStorage.GetSystemVariables(groupIdx)
	if err != nil && err != ErrNotExist {
		log.Error("log storage get fail", log.Int("group_idx", groupIdx), log.Err(err))
		return nil, err
	}
	if err == ErrNotExist {
		log.Error("log storage not found", log.Int("group_idx", groupIdx))
		return nil, ErrNotExist
	}

	variables := &SystemVariables{}
	err = proto.Unmarshal([]byte(buff), variables)
	if err != nil {
		log.Error("buff proto unmarshal fail", log.Err(err))
		return nil, err
	}

	return variables, nil
}
