package paxos

type PaxosLog struct {
	logStore LogStorage
}

func NewPaxosLog(logStorage LogStorage) *PaxosLog {

}

func (l *PaxosLog) WriteLog(options *WriteOptions, groupIdx int, instanceID uint64, value string) error {

}

func (l *PaxosLog) ReadLog(groupIdx int, instanceID uint64) (string, error) {

}

func (l *PaxosLog) GetMaxInstanceIDFromLog(groupIdx int) (uint64, error) {

}

func (l *PaxosLog) WriteState(options *WriteOptions, groupIdx int, instanceID uint64, state *AcceptorStateData) {

}

func (l *PaxosLog) ReadState(groupIdx int, instanceID uint64) (*AcceptorStateData, error) {

}
