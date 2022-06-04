package paxos

import "errors"

var (
	ErrNotExist       = errors.New("not exist")
	ErrDBNotInit      = errors.New("db not init")
	ErrInvalidParam   = errors.New("invalid param")
	ErrInvalidMsg     = errors.New("invalid message")
	ErrMsgTooLarge    = errors.New("msg too large")
	ErrChecksum       = errors.New("checksum fail")
	ErrNotMajority    = errors.New("no majority")
	ErrInvalidOptions = errors.New("invalid options")
)
var ErrUnknown = errors.New("unknown")

var (
	ErrTryCommitReject                     = errors.New("paxos try commit reject")
	ErrTryCommitConflict                   = errors.New("paxos try commit conflict")
	ErrTryCommitExecuteFail                = errors.New("paxos try commit execute fail")
	ErrTryCommitFollowerCannotCommit       = errors.New("paxos try commit follower cannot commit")
	ErrTryCommitNotInMembership            = errors.New("paxos try commit not in membership")
	ErrTryCommitValueSizeTooLarge          = errors.New("paxos try commit value size too large")
	ErrTryCommitTimeout                    = errors.New("paxos try commit timeout")
	ErrTryCommitTooManyThreadWaitingReject = errors.New("paxos try commit too many thread waiting reject")
)

var (
	ErrSystem                         = errors.New("system error")
	ErrGroupIdx                       = errors.New("group idx wrong")
	ErrMembershipOpGidNotSame         = errors.New("membership op gid not same")
	ErrMembershipOpVersionConflict    = errors.New("membership op version conflict")
	ErrMembershipOpNoGid              = errors.New("membership op no gid")
	ErrMembershipOpAddNodeExist       = errors.New("membership op add node exist")
	ErrMembershipOpRemoveNodeNotExist = errors.New("membership op remove node not exist")
	ErrMembershipOpChangeNoChange     = errors.New("membership op change no change")
	ErrGetInstanceValueNotExist       = errors.New("membership value not exist")
	ErrGetInstanceValueNotChosenYet   = errors.New("membership value not chosen yet")
)
