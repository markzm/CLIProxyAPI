package main

import (
	"sync"
	"sync/atomic"
)

// ClientTypeMetrics stores runtime statistics for one client type.
type ClientTypeMetrics struct {
	InFlightRequests       atomic.Int64
	QueueDepth             atomic.Int64
	MaxQueueSize           atomic.Int64
	Capacity               atomic.Int64
	ActiveAuthCount        atomic.Int64
	TotalRequests          atomic.Int64
	AdmittedRequests       atomic.Int64
	FastAdmittedRequests   atomic.Int64
	QueuedAdmittedRequests atomic.Int64
	QueuedRequests         atomic.Int64
	RejectedQueueFull      atomic.Int64
	RejectedGlobalLimit    atomic.Int64
	RejectedTimeout        atomic.Int64
	CanceledInQueue        atomic.Int64
	QueuedWaitMs           atomic.Int64
	MaxWaitMs              atomic.Int64
	WindowRequests         atomic.Int64
	WindowMaxRequests      atomic.Int64
	WindowDurationSeconds  atomic.Int64
}

// RecordQueuedWait records the wait of one request that was queued and then admitted.
func (m *ClientTypeMetrics) RecordQueuedWait(waitMs int64) {
	m.QueuedWaitMs.Add(waitMs)
	for {
		current := m.MaxWaitMs.Load()
		if waitMs <= current || m.MaxWaitMs.CompareAndSwap(current, waitMs) {
			return
		}
	}
}

// ClientTypeSnapshot is a point-in-time snapshot of ClientTypeMetrics.
type ClientTypeSnapshot struct {
	Channel                string  `json:"channel"`
	InFlightRequests       int64   `json:"in_flight_requests"`
	QueueDepth             int64   `json:"queue_depth"`
	MaxQueueSize           int64   `json:"max_queue_size"`
	TotalRequests          int64   `json:"total_requests"`
	AdmittedRequests       int64   `json:"admitted_requests"`
	FastAdmittedRequests   int64   `json:"fast_admitted_requests"`
	QueuedAdmittedRequests int64   `json:"queued_admitted_requests"`
	QueuedRequests         int64   `json:"queued_requests"`
	RejectedQueueFull      int64   `json:"rejected_queue_full"`
	RejectedGlobalLimit    int64   `json:"rejected_global_limit"`
	RejectedTimeout        int64   `json:"rejected_timeout"`
	CanceledInQueue        int64   `json:"canceled_in_queue"`
	AvgWaitMs              float64 `json:"avg_wait_ms"`
	MaxWaitMs              int64   `json:"max_wait_ms"`
	ActiveAuthCount        int64   `json:"active_auth_count"`
	EffectiveCapacity      int64   `json:"effective_capacity"`
	EffectiveMaxQueue      int64   `json:"effective_max_queue"`
	WindowRequests         int64   `json:"window_requests"`
	WindowMaxRequests      int64   `json:"window_max_requests"`
	WindowDurationSeconds  int64   `json:"window_duration_seconds"`
}

// MetricsCollector manages per-client metrics. Entries are created only for configured client types.
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]*ClientTypeMetrics
}

// NewMetricsCollector creates a metrics collector instance.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{metrics: make(map[string]*ClientTypeMetrics)}
}

// ForType returns the metrics instance for a given client type, creating it if needed.
func (m *MetricsCollector) ForType(clientType string) *ClientTypeMetrics {
	m.mu.RLock()
	stat, ok := m.metrics[clientType]
	m.mu.RUnlock()
	if ok {
		return stat
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if stat, ok = m.metrics[clientType]; ok {
		return stat
	}
	stat = &ClientTypeMetrics{}
	m.metrics[clientType] = stat
	return stat
}

// Snapshot returns a snapshot of the given client types; channels maps type -> channel.
func (m *MetricsCollector) Snapshot(channels map[string]string) map[string]ClientTypeSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]ClientTypeSnapshot, len(channels))
	for clientType, channel := range channels {
		stat, ok := m.metrics[clientType]
		if !ok {
			continue
		}
		queuedAdmitted := stat.QueuedAdmittedRequests.Load()
		avgWait := 0.0
		if queuedAdmitted > 0 {
			avgWait = float64(stat.QueuedWaitMs.Load()) / float64(queuedAdmitted)
		}
		maxQueue := stat.MaxQueueSize.Load()
		capacity := stat.Capacity.Load()
		result[clientType] = ClientTypeSnapshot{
			Channel:                channel,
			InFlightRequests:       stat.InFlightRequests.Load(),
			QueueDepth:             stat.QueueDepth.Load(),
			MaxQueueSize:           maxQueue,
			TotalRequests:          stat.TotalRequests.Load(),
			AdmittedRequests:       stat.AdmittedRequests.Load(),
			FastAdmittedRequests:   stat.FastAdmittedRequests.Load(),
			QueuedAdmittedRequests: queuedAdmitted,
			QueuedRequests:         stat.QueuedRequests.Load(),
			RejectedQueueFull:      stat.RejectedQueueFull.Load(),
			RejectedGlobalLimit:    stat.RejectedGlobalLimit.Load(),
			RejectedTimeout:        stat.RejectedTimeout.Load(),
			CanceledInQueue:        stat.CanceledInQueue.Load(),
			AvgWaitMs:              avgWait,
			MaxWaitMs:              stat.MaxWaitMs.Load(),
			ActiveAuthCount:        stat.ActiveAuthCount.Load(),
			EffectiveCapacity:      capacity,
			EffectiveMaxQueue:      maxQueue,
			WindowRequests:         stat.WindowRequests.Load(),
			WindowMaxRequests:      stat.WindowMaxRequests.Load(),
			WindowDurationSeconds:  stat.WindowDurationSeconds.Load(),
		}
	}
	return result
}
