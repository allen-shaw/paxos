package paxos

type MsgTransport interface {
	SendMessage(groupIdx int, sendToNodeID NodeID, buff string) error
	BroadcastMessage(groupIdx int, buff string) error
	BroadcastMessageFollower(groupIdx int, buff string) error
	BroadcastMessageTempNode(groupIdx int, buff string) error
}
