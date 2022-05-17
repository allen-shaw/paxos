package paxos

import "errors"

var (
	ErrNotExist     = errors.New("not exist")
	ErrDBNotInit    = errors.New("db not init")
	ErrInvalidParam = errors.New("invalid param")
)
