package paxos

import (
	"sync"
	"time"
)

type Queue[T any] struct {
	lock    sync.Mutex
	cond    *Cond
	storage queue[T]
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
	value := q.storage.Front()
	return value, true
}

func (q *Queue[T]) Pop() {
	q.storage.PopFront()
}

func (q *Queue[T]) Empty() bool {
	return q.storage.Empty()
}

func (q *Queue[T]) Lock() {
	q.lock.Lock()
}

func (q Queue[T]) Add(value T) int {
	return q.add(value, true, true)
}

func (q *Queue[T]) add(value T, signal bool, back bool) int {
	if back {
		q.storage.PushBack(value)
	} else {
		q.storage.PushFront(value)
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

/////////////////////////////////////////////
type queue[T any] []T

func (q queue[T]) Empty() bool {
	return q.Size() == 0
}

func (q *queue[T]) PopFront() T {
	if q.Empty() {
		return nil
	}
	value := (*q)[0]
	*q = (*q)[1:]
	return value
}

func (q queue[T]) Front() T {
	if q.Empty() {
		return nil
	}
	return q[0]
}

func (q *queue[T]) PushBack(value T) {
	*q = append(*q, value)
}

func (q *queue[T]) PushFront(value T) {
	*q = append([]T{value}, *q...)
}

func (q queue[T]) Size() int {
	return len(q)
}
