package paxos

type WriteOptions struct {
	Sync bool
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{Sync: true}
}

type LogStorage interface {
	GetLogStorageDirPath(groupIdx int) string

	Get(groupIdx int, instanceID uint64) (string, error)

	Put(options *WriteOptions, groupIdx int, instanceID uint64, value string) error

	Del(options *WriteOptions, groupIdx int, instanceID uint64) error

	GetMaxInstanceID(groupIdx int) (uint64, error)

	SetMinChosenInstanceID(options *WriteOptions, groupIdx int, minInstanceID uint64) error

	GetMinChosenInstanceID(groupIdx int) (uint64, error)

	ClearAllLog(groupIdx int) error

	SetSystemVariables(options *WriteOptions, groupIdx int, value string) error

	GetSystemVariables(groupIdx int) (string, error)

	SetMasterVariables(options *WriteOptions, groupIdx int, value string) error

	GetMasterVariables(groupIdx int) (string, error)
}
