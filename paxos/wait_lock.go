package paxos

import "math/rand"

const (
	WaitLockUserTimeAvgInterval = 250
)

type WaitLock struct {
	lock        *SerialLock
	isLookUsing bool

	waitLockCount    int
	maxWaitLockCount int

	lockUseTimeSum   int
	avgLockUseTime   int
	lockUseTimeCount int

	rejectRate              int
	lockWaitTimeThresholdMs int
}

func NewWaitLock() *WaitLock {
	l := &WaitLock{
		lock:                    NewSerialLock(),
		isLookUsing:             false,
		waitLockCount:           0,
		maxWaitLockCount:        -1,
		lockUseTimeSum:          0,
		avgLockUseTime:          0,
		lockUseTimeCount:        0,
		rejectRate:              0,
		lockWaitTimeThresholdMs: -1,
	}
	return l
}

func (l *WaitLock) CanLock() bool {
	if l.maxWaitLockCount != -1 &&
		l.waitLockCount >= l.maxWaitLockCount {
		return false
	}

	if l.lockWaitTimeThresholdMs == -1 {
		return true
	}

	return rand.Intn(100) >= l.rejectRate
}

func (l *WaitLock) RefleshRejectRate(useTimeMs int) {
	if l.lockWaitTimeThresholdMs == -1 {
		return
	}

	l.lockUseTimeSum += useTimeMs
	l.lockUseTimeCount++

	if l.lockUseTimeCount >= WaitLockUserTimeAvgInterval {
		l.avgLockUseTime = l.lockUseTimeSum / l.lockUseTimeCount
		l.lockUseTimeSum = 0
		l.lockUseTimeCount = 0

		if l.avgLockUseTime > l.lockWaitTimeThresholdMs {
			if l.rejectRate != 98 {
				l.rejectRate = l.rejectRate + 3
				if l.rejectRate > 98 {
					l.rejectRate = 98
				}
			} else {
				if l.rejectRate != 0 {
					l.rejectRate = l.rejectRate - 3
					if l.rejectRate < 0 {
						l.rejectRate = 0
					}
				}
			}
		}
	}
}

func (l *WaitLock) SetMaxWaitLockCount(maxWaitLockCount int) {
	l.maxWaitLockCount = maxWaitLockCount
}

func (l *WaitLock) SetLockWaitTimeThresholdMs(lockWaitTimeThresholdMs int) {
	l.lockWaitTimeThresholdMs = lockWaitTimeThresholdMs
}

func (l *WaitLock) Lock(timeoutMs int) (useTimeMs int, ok bool) {
	beginTime := GetCurrentTimeMs()

	l.lock.Lock()
	defer l.lock.Unlock()

	if !l.CanLock() {
		useTimeMs = 0
		return useTimeMs, false
	}

	l.waitLockCount++
	getLock := true

	for l.isLookUsing {
		if timeoutMs == -1 {
			l.lock.WaitTime(1000)
			continue
		} else {
			if !l.lock.WaitTime(timeoutMs) {
				// lock timeout
				getLock = false
				break
			}
		}
	}

	l.waitLockCount--
	endTime := GetCurrentTimeMs()
	useTimeMs = int(endTime - beginTime)
	if useTimeMs < 0 {
		useTimeMs = 0
	}

	l.RefleshRejectRate(useTimeMs)
	if getLock {
		l.isLookUsing = true
	}
	return useTimeMs, getLock
}

func (l *WaitLock) Unlock() {
	l.lock.Lock()
	l.isLookUsing = false
	l.lock.Interrupt()
	l.lock.Unlock()
}

func (l *WaitLock) GetNowHoldThreadCount() int {
	return l.waitLockCount
}

func (l *WaitLock) GetNowAvgThreadWaitTime() int {
	return l.avgLockUseTime
}

func (l *WaitLock) GetNowRejectRate() int {
	return l.rejectRate
}
