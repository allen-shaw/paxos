package logstorage

type LogStorage interface {
	GetLogStorageDirPath(groupIdx int) string

	Get(groupIdx int, instanceId uint64) ([]byte, error)

	Put(options *WriteOptions, groupIdx int, instanceId uint64, value []byte) error

	Del(options *WriteOptions, groupIdx int, instanceId uint64) error

	GetMaxInstanceId(groupIdx int) (uint64, error)

	SetMinChosenInstanceId(options *WriteOptions, groupIdx int, minInstanceId uint64) error

	GetMinChosenInstanceId(groupIdx int) (uint64, error)

	ClearLog(groupIdx int) error

	SetSystemVariables(options *WriteOptions, groupIdx int, value []byte) error

	GetSystemVariables(groupIdx int) ([]byte, error)

	SetMasterVariables(options *WriteOptions, groupIdx int, value []byte) error

	GetMasterVariables(groupIdx int) ([]byte, error)
}
