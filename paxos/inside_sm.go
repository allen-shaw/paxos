package paxos

type InsideSM interface {
	StateMachine
	GetCheckpointBuffer() (string, error)
	UpdateByCheckpoint(buffer string) (bool, error)
}
