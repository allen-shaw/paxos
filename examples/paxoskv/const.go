package paxoskv

import (
	"errors"
	"google.golang.org/grpc/status"
	"math"
)

const (
	KVStatusFail            = 1
	KVStatusKeyNotExist     = 2
	KVStatusVersionConflict = 11
	KVStatusVersionNotExist = 12
	KVStatusRedirect        = 10
	KVStatusNoMaster        = 101
)

var (
	ErrKVStatusFail            = status.Errorf(KVStatusFail, "kv status fail")
	ErrKVStatusKeyNotExist     = status.Errorf(KVStatusKeyNotExist, "kv status key not exist")
	ErrKVStatusVersionConflict = status.Errorf(KVStatusVersionConflict, "kv status version conflict")
	ErrKVStatusVersionNotExist = status.Errorf(KVStatusVersionNotExist, "kv status version not exist")
	ErrKVStatusRedirect        = status.Errorf(KVStatusRedirect, "kv status redirect")
	ErrKVStatusNoMaster        = status.Errorf(KVStatusNoMaster, "kv status no master")
)

var (
	ErrUndefined                  = errors.New("undefined")
	ErrKvClientSysFail            = errors.New("kv client sys fail")
	ErrKvClientKeyNotExist        = errors.New("kv client key not exist")
	ErrKvClientKeyVersionConflict = errors.New("kv client key version conflict")
)

const KvCheckpointKey = math.MaxUint64

const (
	KVOperatorTypeRead   = 1
	KVOperatorTypeWrite  = 2
	KVOperatorTypeDelete = 3
)
