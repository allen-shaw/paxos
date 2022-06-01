package paxos

import (
	"sync"
	"time"
)

type Queue[T any] struct {
	lock    sync.Mutex
	cond    *Cond
	storage []T
}

func NewQueue[T any]() *Queue[T] {
	q := &Queue[T]{}
	q.cond = NewCond(&q.lock)
	q.storage = make([]T, 0)
	return q
}

func (q *Queue[T]) Peek(timeoutMs int) (T, bool) {
	for q.Empty() {
		if q.cond.WaitFor(time.Duration(timeoutMs) * time.Millisecond) {
			return nil, false
		}
	}
	value := q.storage[0]
	q.storage = q.storage[1:]
	return value, true
}

func (q *Queue[T]) Pop() {
	q.storage = q.storage[1:]
}

func (q *Queue[T]) Empty() bool {
	return len(q.storage) == 0
}

func (q *Queue[T]) Lock() {
	q.lock.Lock()
}

func (q Queue[T]) Add(value T) int {
	return q.add(value, true, true)
}

func (q *Queue[T]) add(value T, signal bool, back bool) int {
	if back {
		q.storage = append(q.storage, value)
	} else {
		q.storage = append([]T{value}, q.storage...)
	}

	if signal {
		q.cond.Signal()
	}

	return len(q.storage)
}

func (q *Queue[T]) Unlock() {
	q.lock.Unlock()
}

func (q *Queue[T]) Size() int {
	return len(q.storage)
}
