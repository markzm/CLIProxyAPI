package main

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueFull       = errors.New("queue is full")
	ErrGlobalQueueFull = errors.New("global queue budget exhausted")
	ErrQueueTimeout    = errors.New("queue wait timeout")
)

// QueueBudget bounds the total number and payload bytes of queued requests across all queues.
// Every queued request blocks one native host call (and therefore one OS thread), so this is
// the guard that keeps the process far away from the Go runtime thread limit.
type QueueBudget struct {
	mu         sync.Mutex
	maxWaiters int
	maxBytes   int64
	waiters    int
	bytes      int64
}

// NewQueueBudget creates a budget with the given limits.
func NewQueueBudget(maxWaiters int, maxBytes int64) *QueueBudget {
	b := &QueueBudget{}
	b.SetLimits(maxWaiters, maxBytes)
	return b
}

// SetLimits updates the limits; existing reservations are kept.
func (b *QueueBudget) SetLimits(maxWaiters int, maxBytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maxWaiters = maxWaiters
	b.maxBytes = maxBytes
}

func (b *QueueBudget) tryReserve(size int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiters+1 > b.maxWaiters || b.bytes+size > b.maxBytes {
		return false
	}
	b.waiters++
	b.bytes += size
	return true
}

func (b *QueueBudget) release(size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.waiters--
	b.bytes -= size
}

// Usage returns current and maximum waiter/byte counts.
func (b *QueueBudget) Usage() (waiters, maxWaiters int, bytes, maxBytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.waiters, b.maxWaiters, b.bytes, b.maxBytes
}

type waiter struct {
	ch        chan struct{}
	elem      *list.Element
	size      int64
	grantedAt time.Time
	windowed  bool
}

// DynamicQueue implements a FIFO concurrency-limiting and sliding-window rate-limiting queue.
type DynamicQueue struct {
	clientType string
	budget     *QueueBudget
	metrics    *ClientTypeMetrics

	mu            sync.Mutex
	policy        QueuePolicy
	inFlight      int
	waiters       *list.List
	admittedTimes []time.Time
	windowTimer   *time.Timer

	// mockTime enables a controllable clock for deterministic tests; window timers are not
	// scheduled while it is set and tests advance time through AdvanceTime.
	mockTime time.Time
}

// NewDynamicQueue creates a queue with an initial policy.
func NewDynamicQueue(clientType string, policy QueuePolicy, budget *QueueBudget, metrics *ClientTypeMetrics) *DynamicQueue {
	if budget == nil {
		budget = NewQueueBudget(defaultMaxTotalWaiters, defaultMaxQueuedBytes)
	}
	if metrics == nil {
		metrics = &ClientTypeMetrics{}
	}
	q := &DynamicQueue{
		clientType: clientType,
		budget:     budget,
		metrics:    metrics,
		waiters:    list.New(),
	}
	q.ApplyPolicy(policy)
	return q
}

func normalizePolicy(p QueuePolicy) QueuePolicy {
	if p.Capacity < 1 {
		p.Capacity = 1
	}
	if p.MaxQueue < 0 {
		p.MaxQueue = 0
	}
	if p.WindowMax <= 0 || p.WindowSize <= 0 {
		p.WindowMax = 0
		p.WindowSize = 0
	}
	if p.Timeout <= 0 {
		p.Timeout = defaultQueueTimeoutSeconds * time.Second
	}
	return p
}

// ApplyPolicy installs a complete policy atomically and dispatches exactly once afterwards.
// Window admission history is preserved so a reload cannot reset an exhausted window.
func (q *DynamicQueue) ApplyPolicy(p QueuePolicy) {
	p = normalizePolicy(p)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.policy = p
	q.metrics.MaxQueueSize.Store(int64(p.MaxQueue))
	q.metrics.Capacity.Store(int64(p.Capacity))
	q.metrics.ActiveAuthCount.Store(int64(p.AuthCount))
	q.dispatch()
}

// Policy returns the currently installed policy.
func (q *DynamicQueue) Policy() QueuePolicy {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.policy
}

// RetryAfter suggests how many seconds a rejected client should wait.
func (q *DynamicQueue) RetryAfter() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	if q.windowEnabled() {
		q.cleanExpired(now)
		if len(q.admittedTimes) >= q.policy.WindowMax && len(q.admittedTimes) > 0 {
			seconds := int(q.admittedTimes[0].Add(q.policy.WindowSize).Sub(now)/time.Second) + 1
			if seconds > 1 {
				return seconds
			}
		}
	}
	return 5
}

// RefreshWindowMetrics updates window metrics according to the current time.
func (q *DynamicQueue) RefreshWindowMetrics() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cleanExpired(q.now())
	q.updateMetricsLocked()
}

// SetMockTime injects a controllable clock for deterministic testing.
func (q *DynamicQueue) SetMockTime(t time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mockTime = t
	q.stopWindowTimerLocked()
}

// AdvanceTime advances the mock clock and dispatches any eligible waiters.
func (q *DynamicQueue) AdvanceTime(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mockTime.IsZero() {
		q.mockTime = time.Now()
	}
	q.mockTime = q.mockTime.Add(d)
	q.dispatch()
}

// Close stops background timers.
func (q *DynamicQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopWindowTimerLocked()
}

func (q *DynamicQueue) now() time.Time {
	if !q.mockTime.IsZero() {
		return q.mockTime
	}
	return time.Now()
}

func (q *DynamicQueue) windowEnabled() bool {
	return q.policy.WindowMax > 0 && q.policy.WindowSize > 0
}

func (q *DynamicQueue) cleanExpired(now time.Time) {
	if !q.windowEnabled() {
		return
	}
	cutoff := now.Add(-q.policy.WindowSize)
	idx := 0
	for idx < len(q.admittedTimes) && !q.admittedTimes[idx].After(cutoff) {
		idx++
	}
	if idx > 0 {
		q.admittedTimes = append(q.admittedTimes[:0], q.admittedTimes[idx:]...)
	}
}

func (q *DynamicQueue) canAdmitLocked(now time.Time) bool {
	if q.inFlight >= q.policy.Capacity {
		return false
	}
	if !q.windowEnabled() {
		return true
	}
	q.cleanExpired(now)
	return len(q.admittedTimes) < q.policy.WindowMax
}

// admitLocked takes one slot and returns the window timestamp recorded for it.
func (q *DynamicQueue) admitLocked(now time.Time) (time.Time, bool) {
	q.inFlight++
	if q.windowEnabled() {
		q.admittedTimes = append(q.admittedTimes, now)
		return now, true
	}
	return time.Time{}, false
}

// Acquire requests a concurrency slot, queueing when capacity or window quota is exhausted.
// size is the request payload size charged against the global queue budget while waiting.
// It returns the time spent queued (zero for immediate admission).
func (q *DynamicQueue) Acquire(ctx context.Context, size int64) (time.Duration, error) {
	q.mu.Lock()
	now := q.now()
	q.metrics.TotalRequests.Add(1)

	if q.waiters.Len() == 0 && q.canAdmitLocked(now) {
		q.admitLocked(now)
		q.metrics.AdmittedRequests.Add(1)
		q.metrics.FastAdmittedRequests.Add(1)
		q.updateMetricsLocked()
		q.mu.Unlock()
		return 0, nil
	}

	if q.waiters.Len() >= q.policy.MaxQueue {
		q.metrics.RejectedQueueFull.Add(1)
		q.mu.Unlock()
		return 0, ErrQueueFull
	}
	if !q.budget.tryReserve(size) {
		q.metrics.RejectedGlobalLimit.Add(1)
		q.mu.Unlock()
		return 0, ErrGlobalQueueFull
	}

	w := &waiter{ch: make(chan struct{}, 1), size: size}
	w.elem = q.waiters.PushBack(w)
	q.metrics.QueuedRequests.Add(1)
	q.updateMetricsLocked()
	q.scheduleWindowTimerLocked(now)
	timeout := q.policy.Timeout
	q.mu.Unlock()

	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.ch:
		return q.queuedAdmitted(start), nil
	case <-timer.C:
		q.mu.Lock()
		if q.leaveQueueLocked(w) {
			q.metrics.RejectedTimeout.Add(1)
			q.mu.Unlock()
			return time.Since(start), ErrQueueTimeout
		}
		q.mu.Unlock()
		// Dispatched right as the timer expired.
		return q.queuedAdmitted(start), nil
	case <-ctx.Done():
		q.mu.Lock()
		if !q.leaveQueueLocked(w) {
			// The slot was granted concurrently with cancellation: roll back exactly this grant.
			q.rollbackLocked(w)
		}
		q.metrics.CanceledInQueue.Add(1)
		q.mu.Unlock()
		return time.Since(start), ctx.Err()
	}
}

func (q *DynamicQueue) queuedAdmitted(start time.Time) time.Duration {
	wait := time.Since(start)
	q.metrics.AdmittedRequests.Add(1)
	q.metrics.QueuedAdmittedRequests.Add(1)
	q.metrics.RecordQueuedWait(wait.Milliseconds())
	return wait
}

// leaveQueueLocked removes a still-queued waiter. It returns false if the waiter was already granted.
func (q *DynamicQueue) leaveQueueLocked(w *waiter) bool {
	if w.elem == nil {
		return false
	}
	q.waiters.Remove(w.elem)
	w.elem = nil
	q.budget.release(w.size)
	q.updateMetricsLocked()
	q.scheduleWindowTimerLocked(q.now())
	return true
}

func (q *DynamicQueue) rollbackLocked(w *waiter) {
	if q.inFlight > 0 {
		q.inFlight--
	}
	if w.windowed {
		for i := len(q.admittedTimes) - 1; i >= 0; i-- {
			if q.admittedTimes[i].Equal(w.grantedAt) {
				q.admittedTimes = append(q.admittedTimes[:i], q.admittedTimes[i+1:]...)
				break
			}
		}
	}
	q.dispatch()
}

// Release releases a previously acquired concurrency slot and dispatches the next waiter.
func (q *DynamicQueue) Release() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inFlight > 0 {
		q.inFlight--
	}
	q.dispatch()
}

func (q *DynamicQueue) dispatch() {
	now := q.now()
	for q.waiters.Len() > 0 && q.canAdmitLocked(now) {
		w := q.waiters.Remove(q.waiters.Front()).(*waiter)
		w.elem = nil
		q.budget.release(w.size)
		w.grantedAt, w.windowed = q.admitLocked(now)
		w.ch <- struct{}{}
	}
	q.updateMetricsLocked()
	q.scheduleWindowTimerLocked(now)
}

func (q *DynamicQueue) stopWindowTimerLocked() {
	if q.windowTimer != nil {
		q.windowTimer.Stop()
		q.windowTimer = nil
	}
}

// scheduleWindowTimerLocked wakes the queue when the oldest window admission expires,
// if waiters are blocked only by the window quota.
func (q *DynamicQueue) scheduleWindowTimerLocked(now time.Time) {
	q.stopWindowTimerLocked()
	if !q.mockTime.IsZero() || q.waiters.Len() == 0 || !q.windowEnabled() {
		return
	}
	if q.inFlight >= q.policy.Capacity || len(q.admittedTimes) < q.policy.WindowMax {
		return
	}
	delay := q.admittedTimes[0].Add(q.policy.WindowSize).Sub(now)
	if delay <= 0 {
		delay = time.Millisecond
	}
	q.windowTimer = time.AfterFunc(delay, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.windowTimer = nil
		q.dispatch()
	})
}

func (q *DynamicQueue) updateMetricsLocked() {
	m := q.metrics
	m.InFlightRequests.Store(int64(q.inFlight))
	m.QueueDepth.Store(int64(q.waiters.Len()))
	if q.windowEnabled() {
		m.WindowRequests.Store(int64(len(q.admittedTimes)))
		m.WindowMaxRequests.Store(int64(q.policy.WindowMax))
		m.WindowDurationSeconds.Store(int64(q.policy.WindowSize.Seconds()))
	} else {
		m.WindowRequests.Store(0)
		m.WindowMaxRequests.Store(0)
		m.WindowDurationSeconds.Store(0)
	}
}
