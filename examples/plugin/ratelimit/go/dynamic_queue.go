package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueFull    = errors.New("queue is full")
	ErrQueueTimeout = errors.New("queue wait timeout")
)

type waiter struct {
	ch chan struct{}
}

// DynamicQueue implements a FIFO concurrency-limiting and sliding-window rate-limiting queue.
type DynamicQueue struct {
	clientType string
	mu         sync.Mutex
	capacity   int
	inFlight   int
	waiters    []*waiter
	metrics    *ClientTypeMetrics

	// Dynamic queue size limit
	maxQueueSize int

	// Sliding window rate limiting
	windowSize        time.Duration
	windowMaxRequests int
	admittedTimes     []time.Time
	windowTimer       *time.Timer

	// Controllable clock for deterministic testing
	mockTime time.Time
	nowFunc  func() time.Time
}

// NewDynamicQueue creates a DynamicQueue for a client type with initial capacity.
func NewDynamicQueue(clientType string, initialCapacity int, metrics *ClientTypeMetrics) *DynamicQueue {
	if initialCapacity < 1 {
		initialCapacity = 1
	}
	return &DynamicQueue{
		clientType:    clientType,
		capacity:      initialCapacity,
		maxQueueSize:  500,
		metrics:       metrics,
		waiters:       make([]*waiter, 0),
		admittedTimes: make([]time.Time, 0),
	}
}

// SetMaxQueueSize dynamically updates the maximum allowed queued waiters.
func (q *DynamicQueue) SetMaxQueueSize(newMaxQueue int) {
	if newMaxQueue < 1 {
		newMaxQueue = 1
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxQueueSize = newMaxQueue
	if q.metrics != nil {
		q.metrics.MaxQueueSize.Store(int64(newMaxQueue))
	}
}

// MaxQueueSize returns the current effective maximum queue size.
func (q *DynamicQueue) MaxQueueSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxQueueSize
}

// SetWindowConfig updates the sliding window rate limit configuration.
// If maxReqs <= 0 or windowSize <= 0, window rate limiting is disabled.
func (q *DynamicQueue) SetWindowConfig(windowSize time.Duration, maxReqs int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.windowSize = windowSize
	q.windowMaxRequests = maxReqs
	q.dispatch()
}

// WindowConfig returns the current sliding window duration and max requests limit.
func (q *DynamicQueue) WindowConfig() (time.Duration, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.windowSize, q.windowMaxRequests
}

// SetCapacity dynamically updates the maximum allowed concurrent in-flight requests.
func (q *DynamicQueue) SetCapacity(newCap int) {
	if newCap < 1 {
		newCap = 1
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.capacity = newCap
	q.dispatch()
}

// Capacity returns the current effective capacity.
func (q *DynamicQueue) Capacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

// RefreshWindowMetrics updates metrics according to the current time.
func (q *DynamicQueue) RefreshWindowMetrics() {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	q.cleanExpired(now)
	q.updateWindowMetricsLocked(now)
}

// SetMockTime injects a mock clock for deterministic testing.
func (q *DynamicQueue) SetMockTime(t time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mockTime = t
	now := q.now()
	q.cleanExpired(now)
	q.updateWindowMetricsLocked(now)
}

// AdvanceTime advances mock clock and dispatches any eligible waiters.
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
	if q.windowTimer != nil {
		q.windowTimer.Stop()
		q.windowTimer = nil
	}
}

func (q *DynamicQueue) now() time.Time {
	if !q.mockTime.IsZero() {
		return q.mockTime
	}
	if q.nowFunc != nil {
		return q.nowFunc()
	}
	return time.Now()
}

func (q *DynamicQueue) cleanExpired(now time.Time) {
	if q.windowSize <= 0 || q.windowMaxRequests <= 0 {
		return
	}
	cutoff := now.Add(-q.windowSize)
	idx := 0
	for idx < len(q.admittedTimes) && !q.admittedTimes[idx].After(cutoff) {
		idx++
	}
	if idx > 0 {
		q.admittedTimes = q.admittedTimes[idx:]
	}
}

func (q *DynamicQueue) hasWindowCapacity(now time.Time) bool {
	if q.windowMaxRequests <= 0 || q.windowSize <= 0 {
		return true
	}
	q.cleanExpired(now)
	return len(q.admittedTimes) < q.windowMaxRequests
}

func (q *DynamicQueue) scheduleWindowTimerLocked(now time.Time) {
	if q.windowTimer != nil {
		q.windowTimer.Stop()
		q.windowTimer = nil
	}

	// Controllable clock testing advances time via AdvanceTime
	if !q.mockTime.IsZero() {
		return
	}

	if len(q.waiters) == 0 || q.windowMaxRequests <= 0 || q.windowSize <= 0 {
		return
	}

	if q.inFlight < q.capacity && len(q.admittedTimes) >= q.windowMaxRequests {
		oldest := q.admittedTimes[0]
		expireAt := oldest.Add(q.windowSize)
		delay := expireAt.Sub(now)
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
}

func (q *DynamicQueue) updateWindowMetricsLocked(now time.Time) {
	if q.metrics == nil {
		return
	}
	if q.windowMaxRequests > 0 && q.windowSize > 0 {
		q.metrics.WindowRequests.Store(int64(len(q.admittedTimes)))
		q.metrics.WindowMaxRequests.Store(int64(q.windowMaxRequests))
		q.metrics.WindowDurationSeconds.Store(int64(q.windowSize.Seconds()))
	} else {
		q.metrics.WindowRequests.Store(0)
		q.metrics.WindowMaxRequests.Store(0)
		q.metrics.WindowDurationSeconds.Store(0)
	}
}

// Acquire requests a concurrency slot. If limit or window rate limit is reached, it queues the request.
func (q *DynamicQueue) Acquire(ctx context.Context, requestID string, timeout time.Duration, maxQueue int) error {
	q.mu.Lock()
	now := q.now()

	limit := q.maxQueueSize
	if maxQueue > 0 {
		limit = maxQueue
	}

	// Fast path: capacity available, window quota available, and no queue backlog.
	if q.inFlight < q.capacity && q.hasWindowCapacity(now) && len(q.waiters) == 0 {
		q.inFlight++
		if q.windowMaxRequests > 0 && q.windowSize > 0 {
			q.admittedTimes = append(q.admittedTimes, now)
		}
		if q.metrics != nil {
			q.metrics.InFlightRequests.Store(int64(q.inFlight))
			q.metrics.TotalRequests.Add(1)
			q.metrics.AdmittedRequests.Add(1)
			q.metrics.FastAdmittedRequests.Add(1)
			q.updateWindowMetricsLocked(now)
		}
		q.mu.Unlock()
		return nil
	}

	// Check if queue is full.
	if limit > 0 && len(q.waiters) >= limit {
		if q.metrics != nil {
			q.metrics.TotalRequests.Add(1)
			q.metrics.RejectedQueueFull.Add(1)
			q.updateWindowMetricsLocked(now)
		}
		q.mu.Unlock()
		return ErrQueueFull
	}

	// Enqueue waiter.
	w := &waiter{ch: make(chan struct{}, 1)}
	q.waiters = append(q.waiters, w)
	if q.metrics != nil {
		q.metrics.TotalRequests.Add(1)
		q.metrics.QueuedRequests.Add(1)
		q.metrics.QueueDepth.Store(int64(len(q.waiters)))
		q.updateWindowMetricsLocked(now)
	}

	// If concurrency is available but blocked by window limit, trigger window timer.
	if q.inFlight < q.capacity && !q.hasWindowCapacity(now) {
		q.scheduleWindowTimerLocked(now)
	}
	q.mu.Unlock()

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case <-w.ch:
		if q.metrics != nil {
			q.metrics.AdmittedRequests.Add(1)
			q.metrics.QueuedAdmittedRequests.Add(1)
		}
		return nil
	case <-timeoutCh:
		q.mu.Lock()
		select {
		case <-w.ch:
			// Dispatched right as timer expired.
			if q.metrics != nil {
				q.metrics.AdmittedRequests.Add(1)
				q.metrics.QueuedAdmittedRequests.Add(1)
			}
			q.mu.Unlock()
			return nil
		default:
			q.removeWaiter(w)
			if q.metrics != nil {
				q.metrics.QueueDepth.Store(int64(len(q.waiters)))
				q.metrics.RejectedTimeout.Add(1)
			}
			q.scheduleWindowTimerLocked(q.now())
			q.mu.Unlock()
			return ErrQueueTimeout
		}
	case <-ctx.Done():
		q.mu.Lock()
		select {
		case <-w.ch:
			// Slot was granted before context canceled; release it immediately.
			q.inFlight--
			if len(q.admittedTimes) > 0 {
				q.admittedTimes = q.admittedTimes[:len(q.admittedTimes)-1]
			}
			q.dispatch()
			if q.metrics != nil {
				q.metrics.InFlightRequests.Store(int64(q.inFlight))
				q.metrics.CanceledInQueue.Add(1)
			}
			q.mu.Unlock()
			return ctx.Err()
		default:
			q.removeWaiter(w)
			if q.metrics != nil {
				q.metrics.QueueDepth.Store(int64(len(q.waiters)))
				q.metrics.CanceledInQueue.Add(1)
			}
			q.scheduleWindowTimerLocked(q.now())
			q.mu.Unlock()
			return ctx.Err()
		}
	}
}

// Release releases a previously acquired concurrency slot and dispatches next waiter.
func (q *DynamicQueue) Release(requestID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.inFlight > 0 {
		q.inFlight--
	}
	q.dispatch()
	if q.metrics != nil {
		q.metrics.InFlightRequests.Store(int64(q.inFlight))
	}
}

func (q *DynamicQueue) dispatch() {
	now := q.now()
	if q.windowMaxRequests > 0 && q.windowSize > 0 {
		q.cleanExpired(now)
	}

	for len(q.waiters) > 0 && q.inFlight < q.capacity && (q.windowMaxRequests <= 0 || len(q.admittedTimes) < q.windowMaxRequests) {
		next := q.waiters[0]
		q.waiters = q.waiters[1:]
		q.inFlight++
		if q.windowMaxRequests > 0 && q.windowSize > 0 {
			q.admittedTimes = append(q.admittedTimes, now)
		}
		next.ch <- struct{}{}
	}

	if q.metrics != nil {
		q.metrics.QueueDepth.Store(int64(len(q.waiters)))
		q.metrics.InFlightRequests.Store(int64(q.inFlight))
		q.updateWindowMetricsLocked(now)
	}

	q.scheduleWindowTimerLocked(now)
}

func (q *DynamicQueue) removeWaiter(target *waiter) {
	for i, w := range q.waiters {
		if w == target {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}
