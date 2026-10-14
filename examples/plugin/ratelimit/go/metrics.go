package main

import (
	"sync"
	"sync/atomic"
)

// ClientTypeMetrics stores runtime statistics for one client type.
type ClientTypeMetrics struct {
	InFlightRequests       atomic.Int64 `json:"in_flight_requests"`
	QueueDepth             atomic.Int64 `json:"queue_depth"`
	MaxQueueSize           atomic.Int64 `json:"max_queue_size"`
	TotalRequests          atomic.Int64 `json:"total_requests"`
	AdmittedRequests       atomic.Int64 `json:"admitted_requests"`
	FastAdmittedRequests   atomic.Int64 `json:"fast_admitted_requests"`
	QueuedAdmittedRequests atomic.Int64 `json:"queued_admitted_requests"`
	QueuedRequests         atomic.Int64 `json:"queued_requests"`
	RejectedQueueFull      atomic.Int64 `json:"rejected_queue_full"`
	RejectedTimeout        atomic.Int64 `json:"rejected_timeout"`
	CanceledInQueue        atomic.Int64 `json:"canceled_in_queue"`
	TotalWaitMs            atomic.Int64 `json:"total_wait_ms"`
	MaxWaitMs              atomic.Int64 `json:"max_wait_ms"`
	WindowRequests         atomic.Int64 `json:"window_requests"`
	WindowMaxRequests      atomic.Int64 `json:"window_max_requests"`
	WindowDurationSeconds  atomic.Int64 `json:"window_duration_seconds"`
}

// ClientTypeSnapshot is a point-in-time snapshot of ClientTypeMetrics.
type ClientTypeSnapshot struct {
	InFlightRequests       int64   `json:"in_flight_requests"`
	QueueDepth             int64   `json:"queue_depth"`
	MaxQueueSize           int64   `json:"max_queue_size"`
	TotalRequests          int64   `json:"total_requests"`
	AdmittedRequests       int64   `json:"admitted_requests"`
	FastAdmittedRequests   int64   `json:"fast_admitted_requests"`
	QueuedAdmittedRequests int64   `json:"queued_admitted_requests"`
	QueuedRequests         int64   `json:"queued_requests"`
	RejectedQueueFull      int64   `json:"rejected_queue_full"`
	RejectedTimeout        int64   `json:"rejected_timeout"`
	CanceledInQueue        int64   `json:"canceled_in_queue"`
	AvgWaitMs              float64 `json:"avg_wait_ms"`
	MaxWaitMs              int64   `json:"max_wait_ms"`
	ActiveAuthCount        int     `json:"active_auth_count"`
	EffectiveCapacity      int     `json:"effective_capacity"`
	EffectiveMaxQueue      int     `json:"effective_max_queue"`
	WindowRequests         int64   `json:"window_requests"`
	WindowMaxRequests      int64   `json:"window_max_requests"`
	WindowDurationSeconds  int64   `json:"window_duration_seconds"`
}

// MetricsCollector manages per-client metrics.
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]*ClientTypeMetrics
}

// NewMetricsCollector creates a metrics collector instance.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]*ClientTypeMetrics),
	}
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

// RecordWait records queue waiting duration.
func (m *MetricsCollector) RecordWait(clientType string, waitMs int64) {
	stat := m.ForType(clientType)
	stat.TotalWaitMs.Add(waitMs)
	for {
		currentMax := stat.MaxWaitMs.Load()
		if waitMs <= currentMax || stat.MaxWaitMs.CompareAndSwap(currentMax, waitMs) {
			break
		}
	}
}

// Snapshot returns a point-in-time snapshot map of all tracked client metrics.
func (m *MetricsCollector) Snapshot(capacities map[string]int, authCounts map[string]int, maxQueues map[string]int) map[string]ClientTypeSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]ClientTypeSnapshot, len(m.metrics))
	for clientType, stat := range m.metrics {
		totalReq := stat.TotalRequests.Load()
		queuedReq := stat.QueuedRequests.Load()
		totalWait := stat.TotalWaitMs.Load()

		avgWait := 0.0
		if queuedReq > 0 {
			avgWait = float64(totalWait) / float64(queuedReq)
		}

		effectiveQueue := 0
		if maxQueues != nil {
			effectiveQueue = maxQueues[clientType]
		}
		if effectiveQueue <= 0 {
			effectiveQueue = int(stat.MaxQueueSize.Load())
		}

		result[clientType] = ClientTypeSnapshot{
			InFlightRequests:       stat.InFlightRequests.Load(),
			QueueDepth:             stat.QueueDepth.Load(),
			MaxQueueSize:           stat.MaxQueueSize.Load(),
			TotalRequests:          totalReq,
			AdmittedRequests:       stat.AdmittedRequests.Load(),
			FastAdmittedRequests:   stat.FastAdmittedRequests.Load(),
			QueuedAdmittedRequests: stat.QueuedAdmittedRequests.Load(),
			QueuedRequests:         queuedReq,
			RejectedQueueFull:      stat.RejectedQueueFull.Load(),
			RejectedTimeout:        stat.RejectedTimeout.Load(),
			CanceledInQueue:        stat.CanceledInQueue.Load(),
			AvgWaitMs:              avgWait,
			MaxWaitMs:              stat.MaxWaitMs.Load(),
			ActiveAuthCount:        authCounts[clientType],
			EffectiveCapacity:      capacities[clientType],
			EffectiveMaxQueue:      effectiveQueue,
			WindowRequests:         stat.WindowRequests.Load(),
			WindowMaxRequests:      stat.WindowMaxRequests.Load(),
			WindowDurationSeconds:  stat.WindowDurationSeconds.Load(),
		}
	}
	return result
}
