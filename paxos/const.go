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
