package logstorage

type WriteOptions struct {
	Sync bool
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{Sync: true}
}
