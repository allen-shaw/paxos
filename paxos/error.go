package paxos

import "errors"

var (
	ErrNotExist     = errors.New("not exist")
	ErrDBNotInit    = errors.New("db not init")
	ErrInvalidParam = errors.New("invalid param")
	ErrMsgTooLarge  = errors.New("msg too large")
	ErrChecksum     = errors.New("checksum fail")
	ErrNotMajority  = errors.New("no majority")
)
