package paxos

import "unsafe"

const (
	SystemVSMID      = 100000000
	MasterVSMID      = 100000001
	BatchProposeSMID = 100000002
)

var (
	sizeOfInt    = int(unsafe.Sizeof(int(0)))
	sizeOfUint16 = int(unsafe.Sizeof(uint16(0)))
	sizeOfUint32 = int(unsafe.Sizeof(uint32(0)))
	sizeOfUint64 = int(unsafe.Sizeof(uint64(0)))
)

// MsgCmd
const (
	MsgCmdPaxosMsg      = 1
	MsgCmdCheckpointMsg = 2
)

// PaxosMsgType
const (
	MsgTypePaxosPrepare                    = 1
	MsgTypePaxosPrepareReply               = 2
	MsgTypePaxosAccept                     = 3
	MsgTypePaxosAcceptReply                = 4
	MsgTypePaxosLearnerAskForLearn         = 5
	MsgTypePaxosLearnerSendLearnValue      = 6
	MsgTypePaxosLearnerProposerSendSuccess = 7
	MsgTypePaxosProposalSendNewValue       = 8
	MsgTypePaxosLearnerSendNowInstanceID   = 9
	MsgTypePaxosLearnerComfirmAskForLearn  = 10
	MsgTypePaxosLearnerSendLearnValueAck   = 11
	MsgTypePaxosLearnerAskForCheckpoint    = 12
	MsgTypePaxosLearnerOnAskForCheckpoint  = 13
)

type TimerType int

const (
	TimerProposerPrepareTimeout TimerType = 1
	TimerProposerAcceptTimeout            = 2
	TimerLearnerAskForLearnNoop           = 3
	TimerInstanceCommitTimeout            = 4
)

type PaxosTryCommitRet int

const (
	PaxosTryCommitRetOk                         PaxosTryCommitRet = 0
	PaxosTryCommitRetReject                                       = -2
	PaxosTryCommitRetConflict                                     = 14
	PaxosTryCommitRetExecuteFail                                  = 15
	PaxosTryCommitRetFollowerCannotCommit                         = 16
	PaxosTryCommitRetNotInMembership                              = 17
	PaxosTryCommitRetValueSizeTooLarge                            = 18
	PaxosTryCommitRetTimeout                                      = 404
	PaxosTryCommitRetTooManyThreadWaitingReject                   = 405
)

type PaxosNodeFunctionRet int

const (
	PaxosSystemError                       PaxosNodeFunctionRet = -1
	PaxosGroupIdxWrong                                          = -5
	PaxosMembershipOpGidNotSame                                 = -501
	PaxosMembershipOpVersionConflit                             = -502
	PaxosMembershipOpAddNodeExist                               = 1002
	PaxosMembershipOpNoGid                                      = 1001
	PaxosMembershipOpRemoveNodeNotExist                         = 1003
	PaxosMembershipOpChangeNoChange                             = 1004
	PaxosGetInstanceValueValueNotExist                          = 1005
	PaxosGetInstanceValueValueNotChosenYet                      = 1006
)

type PaxosMsgFlagType int

const (
	PaxosMsgFlagTypeSendLearnValueNeedAck = 1
)

type CheckpointMsgType int

const (
	CheckpointMsgTypeSendFile    = 1
	CheckpointMsgTypeSendFileAck = 2
)

type CheckpointSendFileFlag int

const (
	CheckpointSendFileFlagBEGIN = 1
	CheckpointSendFileFlagING   = 2
	CheckpointSendFileFlagEND   = 3
)

type CheckpointSendFileAckFlag int

const (
	CheckpointSendFileAckFlagOK   = 1
	CheckpointSendFileAckFlagFail = 2
)
