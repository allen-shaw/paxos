package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
)

type MasterVariablesStore struct {
	logStorage LogStorage
}

func NewMasterVariablesStore(logStorage LogStorage) *MasterVariablesStore {
	return &MasterVariablesStore{
		logStorage: logStorage,
	}
}

func (s *MasterVariablesStore) Write(options *WriteOptions, groupIdx int, variables *MasterVariables) error {
	buff, err := proto.Marshal(variables)
	if err != nil {
		log.Error("variables marshal fail", log.Err(err))
		return err
	}

	err = s.logStorage.SetMasterVariables(options, groupIdx, string(buff))
	if err != nil {
		log.Error("log storage set master variables fail", log.Int("group_idx", groupIdx), log.Int("buffer size", len(buff)), log.Err(err))
		return err
	}

	return nil
}

func (s *MasterVariablesStore) Read(groupIdx int) (*MasterVariables, error) {
	buff, err := s.logStorage.GetMasterVariables(groupIdx)
	if err != nil && err != ErrNotExist {
		log.Error("log storage get fail", log.Int("group_idx", groupIdx), log.Err(err))
		return nil, err
	}
	if err == ErrNotExist {
		log.Error("log storage not found", log.Int("group_idx", groupIdx))
		return nil, ErrNotExist
	}

	variables := &MasterVariables{}
	err = proto.Unmarshal([]byte(buff), variables)
	if err != nil {
		log.Error("buff proto unmarshal fail", log.Err(err))
		return nil, err
	}

	return variables, nil
}
