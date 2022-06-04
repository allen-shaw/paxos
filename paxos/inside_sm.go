package paxos

type InsideSM interface {
	StateMachine
	GetCheckpointBuffer() (string, error)
	UpdateByCheckpoint(buffer []byte) (bool, error)
}
