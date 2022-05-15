package paxos

type WriteOptions struct {
	Sync bool
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{Sync: true}
}

type LogStorage interface {
	GetLogStorageDirPath(groupIdx int)
	Get(groupIdx int, instanceID uint64, value string) int
	Put(options *WriteOptions, groupIdx int, instanceID uint64, value string) int
	Del(options *WriteOptions, groupIdx int, instanceID uint64) int
	GetMaxInstanceID(groupIdx int, instanceID uint64) int
	SetMinChosenInstanceID(options *WriteOptions, groupIdx int, instanceID uint64) int
	GetMinChosenInstanceID(groupIdx int, instanceID uint64) int
	ClearAllLog(groupIdx int) int
	SetSystemVariables(options *WriteOptions, groupIdx int, buf string) int
	GetSystemVariables(groupIdx int, buf string) int
	SetMasterVariables(options *WriteOptions, groupIdx int, buf string) int
	GetMasterVariables(groupIdx int, buf string) int
}
