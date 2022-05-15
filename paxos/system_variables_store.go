package paxos

type SystemVariablesStore struct {
	logStorage LogStorage
}

func NewSystemVariablesStore(logStorage LogStorage) *SystemVariablesStore {
	return &SystemVariablesStore{logStorage: logStorage}
}

func (s *SystemVariablesStore) Write(options *WriteOptions, groupIdx int, variables *SystemVariables) error {

}

func (s *SystemVariablesStore) Read(groupIdx int) (*SystemVariables, error) {

}
