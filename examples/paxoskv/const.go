package paxoskv

import (
	"errors"
	"math"
)

var (
	ErrUndefined                  = errors.New("undefined")
	ErrKvClientSysFail            = errors.New("kv client sys fail")
	ErrKvClientKeyNotExist        = errors.New("kv client key not exist")
	ErrKvClientKeyVersionConflict = errors.New("kv client key version conflict")
)

const KvCheckpointKey = math.MaxUint64
