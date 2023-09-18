package paxos

type Pair[T1 any, T2 any] struct {
	First  T1
	Second T2
}

func MakePair[T1 any, T2 any](first T1, second T2) *Pair[T1, T2] {
	return &Pair[T1, T2]{First: first, Second: second}
}
