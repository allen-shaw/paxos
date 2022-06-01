package paxos

import (
	"container/heap"
)

type timer struct {
	TimerID uint32
	AbsTime uint64
	Type    int
}

func newTimer(timerID uint32, absTime uint64, timerType int) *timer {
	return &timer{TimerID: timerID, AbsTime: absTime, Type: timerType}
}

func (t *timer) Less(rhs *timer) bool {
	if rhs.AbsTime == t.AbsTime {
		return rhs.TimerID < t.TimerID
	}
	return rhs.AbsTime < t.AbsTime
}

type timerHeap []*timer

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timerHeap) Push(x interface{}) {
	*h = append(*h, x.(*timer))
}

func (h *timerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Timer struct {
	nowTimerID uint32
	timerHeap  *timerHeap
}

func NewTimer() *Timer {
	h := &timerHeap{}
	heap.Init(h)
	return &Timer{
		nowTimerID: 0,
		timerHeap:  h,
	}
}

func (t *Timer) AddTimer(absTime uint64) (timerID uint32) {
	return t.AddTimerWithType(absTime, 0)
}

func (t *Timer) AddTimerWithType(absTime uint64, timerType int) (timerID uint32) {
	t.nowTimerID++
	timerID = t.nowTimerID

	tim := newTimer(timerID, absTime, timerType)
	heap.Push(t.timerHeap, tim)

	return timerID
}

func (t *Timer) PopTimeout() (timerID uint32, timerType int, ok bool) {
	if t.timerHeap.Len() == 0 {
		return 0, 0, false
	}

	tim := heap.Pop(t.timerHeap).(*timer)
	nowTime := GetCurrentTimeMs()

	if tim.AbsTime > nowTime {
		heap.Push(t.timerHeap, tim)
		return 0, 0, false
	}

	return tim.TimerID, tim.Type, true
}

func (t *Timer) GetNextTimeout() int {
	if t.timerHeap.Len() == 0 {
		return -1
	}
	nextTimeout := 0

	tim := heap.Pop(t.timerHeap).(*timer)
	nowTime := GetCurrentTimeMs()

	if tim.AbsTime > nowTime {
		heap.Push(t.timerHeap, tim)
		nextTimeout = int(tim.AbsTime - nowTime)
	}
	return nextTimeout
}
