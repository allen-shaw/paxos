package paxos

import (
	"encoding/binary"
	"errors"
	"github.com/AllenShaw19/paxos/log"
	"github.com/golang/protobuf/proto"
)

var (
	GroupIdxLen = sizeOfInt
	HeadlenLen  = sizeOfUint16
	ChecksumLen = sizeOfUint32
)

type BallotNumber struct {
	ProposalID uint64
	NodeID     NodeID
}

func NewBallotNumber(proposalID uint64, nodeID NodeID) *BallotNumber {
	return &BallotNumber{
		ProposalID: proposalID,
		NodeID:     nodeID,
	}
}

func (n *BallotNumber) Equal(other *BallotNumber) bool {
	return n.ProposalID == other.ProposalID && n.NodeID == other.NodeID
}

func (n *BallotNumber) Large(other *BallotNumber) bool {
	if n.ProposalID == other.ProposalID {
		return n.NodeID > other.NodeID
	}
	return n.ProposalID > other.ProposalID
}

func (n *BallotNumber) LargeOrEqual(other *BallotNumber) bool {
	if n.ProposalID == other.ProposalID {
		return n.NodeID >= other.NodeID
	}
	return n.ProposalID >= other.ProposalID
}

func (n *BallotNumber) IsNull() bool {
	return n.ProposalID == 0
}

func (n *BallotNumber) Reset() {
	n.ProposalID = 0
	n.NodeID = nilNode
}

///////////////////////////////////////////////////////////

type BroadcastMessageType int

const (
	BroadcastMessageTypeRunSelfFirst BroadcastMessageType = 1
	BroadcastMessageTypeRunSelfFinal                      = 2
	BroadcastMessageTypeRunSelfNone                       = 3
)

///////////////////////////////////////////////////////////

type base struct {
	config       *Config
	msgTransport MsgTransport
	instance     *Instance
	instanceID   uint64
	isTestMode   bool
}

func newBase(config *Config, msgTransport MsgTransport, instance *Instance) *base {
	return &base{
		config:       config,
		msgTransport: msgTransport,
		instance:     instance,
		instanceID:   0,
		isTestMode:   false,
	}
}

func (b *base) GetInstanceID() uint64 {
	return b.instanceID
}

func (b *base) SetInstanceID(instanceID uint64) {
	b.instanceID = instanceID
}

func (b *base) PackMsg(msg *PaxosMsg) (string, error) {
	bodyBuffer, err := proto.Marshal(msg)
	if err != nil {
		log.Error("paxos msg marshal fail. skip", log.Err(err))
		return "", err
	}

	cmd := MsgCmdPaxosMsg
	buff := b.PackBaseMsg(string(bodyBuffer), cmd)
	return buff, nil
}

func (b *base) PackCheckpointMsg(msg *CheckpointMsg) (string, error) {
	bodyBuffer, err := proto.Marshal(msg)
	if err != nil {
		log.Error("checkpoint msg marshal fail. skip", log.Err(err))
		return "", err
	}

	cmd := MsgCmdCheckpointMsg
	buff := b.PackBaseMsg(string(bodyBuffer), cmd)
	return buff, nil
}

func (b *base) GetLastChecksum() uint32 {
	return b.instance.GetLastChecksum()
}

func (b *base) PackBaseMsg(bodyBuffer string, cmd int) string {
	groupIdx := b.config.GetMyGroupIdx()
	groupIdxBuffer := IntToBytes(groupIdx)

	header := &Header{}
	header.Gid = b.config.GetGid()
	header.Rid = 0
	header.CmdId = int32(cmd)
	header.Version = 1

	headerBuffer := make([]byte, 0)
	err := proto.Unmarshal(headerBuffer, header)
	if err != nil {
		log.Error("header unmarshal fail, skip", log.Err(err))
		panic(err)
	}

	headerLen := uint16(len(headerBuffer))
	headerLenBuffer := IntToBytes(headerLen)

	buffer := make([]byte, 0)
	buffer = append(buffer, groupIdxBuffer...)
	buffer = append(buffer, headerLenBuffer...)
	buffer = append(buffer, headerBuffer...)
	buffer = append(buffer, bodyBuffer...)

	bufferChecksum := Crc32(buffer)
	checksumBuffer := IntToBytes(bufferChecksum)

	buffer = append(buffer, checksumBuffer...)
	return string(buffer)
}

func (b *base) UnpackBaseMsg(buff string) (header *Header, bodyStartPos int, bodyLen int, err error) {
	return UnpackBaseMsg(buff)
}

func (b *base) SetAsTestMode() {
	b.isTestMode = true
}

func (b *base) sendMessage(sendToNodeId NodeID, paxosMsg *PaxosMsg) error {
	if b.isTestMode {
		return nil
	}

	if sendToNodeId == b.config.GetMyNodeID() {
		err := b.instance.OnReceivePaxosMsg(paxosMsg, false)
		if err != nil {
			return err
		}
	}

	msg, err := b.PackMsg(paxosMsg)
	if err != nil {
		return err
	}
	return b.msgTransport.SendMessage(b.config.GetMyGroupIdx(), sendToNodeId, msg)
}

func (b *base) broadcastMessage(paxosMsg *PaxosMsg, runType BroadcastMessageType) error {
	if b.isTestMode {
		return nil
	}

	if runType == BroadcastMessageTypeRunSelfFirst {
		err := b.instance.OnReceivePaxosMsg(paxosMsg, false)
		if err != nil {
			return err
		}
	}

	msg, err := b.PackMsg(paxosMsg)
	if err != nil {
		return err
	}

	err = b.msgTransport.BroadcastMessage(b.config.GetMyGroupIdx(), msg)
	if runType == BroadcastMessageTypeRunSelfFinal {
		b.instance.OnReceivePaxosMsg(paxosMsg, false)
	}

	return err
}

func (b *base) broadcastMessageToFollower(paxosMsg *PaxosMsg) error {
	msg, err := b.PackMsg(paxosMsg)
	if err != nil {
		return err
	}
	return b.msgTransport.BroadcastMessageFollower(b.config.GetMyGroupIdx(), msg)
}

func (b *base) broadcastMessageToTempNode(paxosMsg *PaxosMsg) error {
	msg, err := b.PackMsg(paxosMsg)
	if err != nil {
		return err
	}
	return b.msgTransport.BroadcastMessageTempNode(b.config.GetMyGroupIdx(), msg)
}

func (b *base) sendCheckpointMsg(sendToNodeId NodeID, checkpointMsg *CheckpointMsg) error {
	if sendToNodeId == b.config.GetMyNodeID() {
		return nil
	}

	msg, err := b.PackCheckpointMsg(checkpointMsg)
	if err != nil {
		return err
	}
	return b.msgTransport.SendMessage(b.config.GetMyGroupIdx(), sendToNodeId, msg)
}

///////////////////////////////////////////////////////

func UnpackBaseMsg(buff string) (header *Header, bodyStartPos int, bodyLen int, err error) {
	headerLenBuffer := buff[GroupIdxLen : GroupIdxLen+HeadlenLen]
	headerLen := int(binary.BigEndian.Uint16([]byte(headerLenBuffer)))

	headerStartPos := GroupIdxLen + HeadlenLen
	bodyStartPos = headerStartPos + int(headerLen)

	if bodyStartPos > len(buff) {
		log.Error("header headerlen too long", log.Int("header_len", headerLen))
		return nil, 0, 0, errors.New("invalid header")
	}

	headerBuffer := buff[headerStartPos : headerStartPos+headerLen]
	err = proto.Unmarshal([]byte(headerBuffer), header)
	if err != nil {
		log.Error("ummarshal header fail. skip", log.Err(err))
		return nil, 0, 0, err
	}

	log.Debug("unpack base msg", log.Int("buffer_size", len(buff)),
		log.Int("header_len", headerLen),
		log.Int32("cmdid", header.CmdId),
		log.Uint64("gid", header.Gid),
		log.Uint64("rid", header.Rid),
		log.Int32("version", header.Version),
		log.Int("body_startpos", bodyStartPos))

	if header.Version >= 1 {
		if bodyStartPos+ChecksumLen > len(buff) {
			log.Error("no checksum", log.Int("body_start_pos", bodyStartPos), log.Int("buffer_size", len(buff)))
			return nil, 0, 0, errors.New("invalid buffer")
		}

		bodyLen = len(buff) - ChecksumLen - bodyStartPos

		checksumBuffer := buff[len(buff)-ChecksumLen:]
		bufferChecksum := binary.BigEndian.Uint32([]byte(checksumBuffer))

		calcBufferChecksum := Crc32(buff[:len(buff)-ChecksumLen])
		if calcBufferChecksum != bufferChecksum {
			log.Error("checksum fail", log.Uint32("data_checksum", bufferChecksum),
				log.Uint32("calc_checksum", calcBufferChecksum))
			return nil, 0, 0, ErrChecksum
		}
	} else {
		bodyLen = len(buff) - bodyStartPos
	}

	return header, bodyStartPos, bodyLen, nil
}
