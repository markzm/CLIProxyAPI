package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestClassifier(t *testing.T) {
	c := NewClassifier()

	tests := []struct {
		name         string
		headers      http.Header
		sourceFormat string
		path         string
		expected     string
	}{
		{
			name:     "Explicit header takes top priority",
			headers:  http.Header{"X-Client-Type": []string{"custom-worker"}, "User-Agent": []string{"Codex Desktop/1.0"}},
			expected: "custom-worker",
		},
		{
			name:     "Codex Desktop user agent",
			headers:  http.Header{"User-Agent": []string{"Codex Desktop/0.144.0"}},
			expected: "codex",
		},
		{
			name:     "Codex CLI user agent",
			headers:  http.Header{"User-Agent": []string{"codex_cli_rs/0.150.0"}},
			expected: "codex",
		},
		{
			name:     "Claude Code user agent",
			headers:  http.Header{"User-Agent": []string{"Claude Code/1.0.0"}},
			expected: "claude-code",
		},
		{
			name:     "Antigravity user agent",
			headers:  http.Header{"User-Agent": []string{"antigravity-cli/2.0"}},
			expected: "gemini",
		},
		{
			name:     "Cursor IDE plugin",
			headers:  http.Header{"User-Agent": []string{"Cursor/0.40.0"}},
			expected: "ide-plugin",
		},
		{
			name:     "Python requests bot",
			headers:  http.Header{"User-Agent": []string{"python-requests/2.31.0"}},
			expected: "script-bot",
		},
		{
			name:         "Path fallback for codex responses",
			headers:      http.Header{"User-Agent": []string{"unknown-client"}},
			path:         "/v1/responses",
			sourceFormat: "openai-response",
			expected:     "codex",
		},
		{
			name:         "Path fallback for claude messages",
			headers:      http.Header{},
			path:         "/v1/messages",
			sourceFormat: "claude",
			expected:     "claude-code",
		},
		{
			name:     "Default fallback",
			headers:  http.Header{"User-Agent": []string{"unknown"}},
			path:     "/v1/chat/completions",
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.Resolve(tt.headers, tt.sourceFormat, tt.path)
			if got != tt.expected {
				t.Errorf("Resolve() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestDynamicQueue_AcquireAndRelease(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("test")
	q := NewDynamicQueue("test", 2, stat)

	ctx := context.Background()

	// 1. First 2 acquisitions should succeed immediately
	if err := q.Acquire(ctx, "req-1", 100*time.Millisecond, 5); err != nil {
		t.Fatalf("req-1 Acquire failed: %v", err)
	}
	if err := q.Acquire(ctx, "req-2", 100*time.Millisecond, 5); err != nil {
		t.Fatalf("req-2 Acquire failed: %v", err)
	}

	if q.inFlight != 2 {
		t.Errorf("expected inFlight=2, got %d", q.inFlight)
	}

	// 2. Third acquisition should block and be queued
	acquired := make(chan bool)
	go func() {
		err := q.Acquire(ctx, "req-3", time.Second, 5)
		acquired <- (err == nil)
	}()

	// Give goroutine a moment to enter queue
	time.Sleep(10 * time.Millisecond)
	if q.metrics.QueueDepth.Load() != 1 {
		t.Errorf("expected QueueDepth=1, got %d", q.metrics.QueueDepth.Load())
	}

	// 3. Release req-1, req-3 should immediately be dispatched
	q.Release("req-1")

	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("req-3 failed to acquire after release")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for req-3 to be dispatched")
	}

	// Clean up
	q.Release("req-2")
	q.Release("req-3")

	if q.inFlight != 0 {
		t.Errorf("expected inFlight=0 after all releases, got %d", q.inFlight)
	}
}

func TestDynamicQueue_QueueTimeout(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("timeout-test")
	q := NewDynamicQueue("timeout-test", 1, stat)

	ctx := context.Background()
	_ = q.Acquire(ctx, "req-1", time.Second, 2)

	// Second request should time out
	err := q.Acquire(ctx, "req-2", 30*time.Millisecond, 2)
	if err != ErrQueueTimeout {
		t.Errorf("expected ErrQueueTimeout, got %v", err)
	}

	if stat.RejectedTimeout.Load() != 1 {
		t.Errorf("expected RejectedTimeout=1, got %d", stat.RejectedTimeout.Load())
	}

	q.Release("req-1")
}

func TestDynamicQueue_QueueFull(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("full-test")
	q := NewDynamicQueue("full-test", 1, stat)

	ctx := context.Background()
	_ = q.Acquire(ctx, "req-1", time.Second, 1)

	// Enqueue req-2 to fill the queue (maxQueue = 1)
	go func() {
		_ = q.Acquire(ctx, "req-2", time.Second, 1)
	}()

	time.Sleep(10 * time.Millisecond)

	// req-3 should immediately be rejected because queue is full
	err := q.Acquire(ctx, "req-3", time.Second, 1)
	if err != ErrQueueFull {
		t.Errorf("expected ErrQueueFull, got %v", err)
	}

	if stat.RejectedQueueFull.Load() != 1 {
		t.Errorf("expected RejectedQueueFull=1, got %d", stat.RejectedQueueFull.Load())
	}

	q.Release("req-1")
	time.Sleep(10 * time.Millisecond)
	q.Release("req-2")
}

func TestDynamicQueue_ContextCancellation(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("cancel-test")
	q := NewDynamicQueue("cancel-test", 1, stat)

	// Saturate capacity
	_ = q.Acquire(context.Background(), "req-1", time.Second, 5)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- q.Acquire(ctx, "req-2", time.Second, 5)
	}()

	time.Sleep(10 * time.Millisecond)
	// Cancel downstream context
	cancel()

	err := <-errCh
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	if stat.CanceledInQueue.Load() != 1 {
		t.Errorf("expected CanceledInQueue=1, got %d", stat.CanceledInQueue.Load())
	}

	q.Release("req-1")
}

func TestDynamicQueue_CapacityScaleUp(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("scale-test")
	q := NewDynamicQueue("scale-test", 1, stat)

	_ = q.Acquire(context.Background(), "req-1", time.Second, 5)

	acquired := make(chan bool)
	go func() {
		err := q.Acquire(context.Background(), "req-2", time.Second, 5)
		acquired <- (err == nil)
	}()

	time.Sleep(10 * time.Millisecond)
	if q.metrics.QueueDepth.Load() != 1 {
		t.Errorf("expected QueueDepth=1, got %d", q.metrics.QueueDepth.Load())
	}

	// Dynamically scale up capacity from 1 to 2 -> req-2 should be dispatched immediately!
	q.SetCapacity(2)

	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("req-2 failed to acquire after capacity increase")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for waiter after capacity scale up")
	}

	q.Release("req-1")
	q.Release("req-2")
}

func TestAuthSyncManager_DynamicCalculation(t *testing.T) {
	// Mock host.auth.list response
	mockHostCall := func(method string, _ any) (json.RawMessage, error) {
		if method == "host.auth.list" {
			data := authListResponse{
				Files: []pluginapi.HostAuthFileEntry{
					{Name: "codex-1.json", Provider: "codex", Disabled: false, Status: "active"},
					{Name: "codex-2.json", Provider: "codex", Disabled: false, Status: "active"},
					{Name: "codex-3.json", Provider: "codex", Disabled: true, Status: "disabled"}, // Disabled, should not count
					{Name: "claude-1.json", Provider: "claude", Disabled: false, Status: "active"},
					{Name: "gemini-1.json", Provider: "gemini", Disabled: false, Status: "active"},
				},
			}
			raw, _ := json.Marshal(data)
			return json.RawMessage(raw), nil
		}
		return nil, nil
	}

	syncMgr := NewAuthSyncManager(mockHostCall)
	cfg := DefaultPluginConfig()
	cfg.ChannelPerAuth["codex"] = 5
	cfg.ChannelPerAuth["claude"] = 4
	cfg.ChannelPerAuth["gemini"] = 10
	cfg.ChannelQueuePerAuth["codex"] = 150
	cfg.ChannelQueuePerAuth["claude"] = 200
	cfg.ChannelWindowPerAuth["codex"] = 50

	queues := map[string]*DynamicQueue{
		"codex":       NewDynamicQueue("codex", 1, nil),
		"claude-code": NewDynamicQueue("claude-code", 1, nil),
		"gemini":      NewDynamicQueue("gemini", 1, nil),
	}

	syncMgr.SyncActiveAuths(cfg, queues, nil)

	// Check calculated capacities
	// codex: 2 active * 5 = 10
	if got := queues["codex"].Capacity(); got != 10 {
		t.Errorf("codex capacity = %d, want 10", got)
	}
	// claude: 1 active * 4 = 4
	if got := queues["claude-code"].Capacity(); got != 4 {
		t.Errorf("claude capacity = %d, want 4", got)
	}
	// gemini: 1 active * 10 = 10
	if got := queues["gemini"].Capacity(); got != 10 {
		t.Errorf("gemini capacity = %d, want 10", got)
	}

	// Check dynamic queue length
	// codex: 2 active * 150 = 300
	if got := queues["codex"].MaxQueueSize(); got != 300 {
		t.Errorf("codex max queue size = %d, want 300", got)
	}
	// claude: 1 active * 200 = 200
	if got := queues["claude-code"].MaxQueueSize(); got != 200 {
		t.Errorf("claude max queue size = %d, want 200", got)
	}
	// gemini: 1 active * default 100 = 100
	if got := queues["gemini"].MaxQueueSize(); got != 100 {
		t.Errorf("gemini max queue size = %d, want 100", got)
	}

	// Check dynamic window limit
	// codex: 2 active * 50 = 100
	_, codexWindowMax := queues["codex"].WindowConfig()
	if codexWindowMax != 100 {
		t.Errorf("codex window limit = %d, want 100", codexWindowMax)
	}

	authCounts, caps, maxQueues, windowLimits := syncMgr.GetSnapshot()
	if authCounts["codex"] != 2 {
		t.Errorf("active auth count for codex = %d, want 2", authCounts["codex"])
	}
	if caps["codex"] != 10 {
		t.Errorf("effective capacity for codex = %d, want 10", caps["codex"])
	}
	if maxQueues["codex"] != 300 {
		t.Errorf("effective max queue for codex = %d, want 300", maxQueues["codex"])
	}
	if windowLimits["codex"] != 100 {
		t.Errorf("effective window limit for codex = %d, want 100", windowLimits["codex"])
	}
}

func TestDynamicQueue_MetricsAccounting(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("accounting-test")
	q := NewDynamicQueue("accounting-test", 1, stat)

	// 1. First request acquires directly (fast path)
	if err := q.Acquire(context.Background(), "req-1", time.Second, 5); err != nil {
		t.Fatalf("req-1 Acquire failed: %v", err)
	}
	if stat.FastAdmittedRequests.Load() != 1 || stat.AdmittedRequests.Load() != 1 {
		t.Errorf("fast admitted expected 1, got %d / %d", stat.FastAdmittedRequests.Load(), stat.AdmittedRequests.Load())
	}

	// 2. Second request enters queue and waits
	done2 := make(chan error, 1)
	go func() {
		done2 <- q.Acquire(context.Background(), "req-2", time.Second, 5)
	}()
	time.Sleep(10 * time.Millisecond)

	if stat.QueuedRequests.Load() != 1 || stat.QueueDepth.Load() != 1 {
		t.Errorf("queued expected 1, depth 1, got queued=%d depth=%d", stat.QueuedRequests.Load(), stat.QueueDepth.Load())
	}

	// Release req-1 -> req-2 should be admitted from queue!
	q.Release("req-1")
	if err := <-done2; err != nil {
		t.Fatalf("req-2 failed to acquire: %v", err)
	}

	if stat.QueuedAdmittedRequests.Load() != 1 {
		t.Errorf("queued admitted expected 1, got %d", stat.QueuedAdmittedRequests.Load())
	}
	if stat.AdmittedRequests.Load() != 2 {
		t.Errorf("total admitted expected 2, got %d", stat.AdmittedRequests.Load())
	}

	// 3. Third request times out
	err3 := q.Acquire(context.Background(), "req-3", 20*time.Millisecond, 5)
	if err3 != ErrQueueTimeout {
		t.Errorf("expected ErrQueueTimeout, got %v", err3)
	}
	if stat.RejectedTimeout.Load() != 1 {
		t.Errorf("rejected timeout expected 1, got %d", stat.RejectedTimeout.Load())
	}

	// 4. Verify mathematical formula: Total = Admitted + RejectedTimeout + QueueDepth
	tot := stat.TotalRequests.Load()
	adm := stat.AdmittedRequests.Load()
	rej := stat.RejectedTimeout.Load() + stat.RejectedQueueFull.Load()
	depth := stat.QueueDepth.Load()

	if tot != adm+rej+depth {
		t.Fatalf("accounting mismatch: Total=%d != Admitted(%d) + Rejected(%d) + Depth(%d)", tot, adm, rej, depth)
	}

	q.Release("req-2")
}

func TestDynamicQueue_SlidingWindowRateLimit(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("window-test")
	q := NewDynamicQueue("window-test", 5, stat)

	// Set window limit: max 2 requests per 10 minutes
	windowDuration := 10 * time.Minute
	q.SetWindowConfig(windowDuration, 2)

	baseTime := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	q.SetMockTime(baseTime)

	ctx := context.Background()

	// 1. Acquire first 2 requests (fast path, consumes window quota)
	if err := q.Acquire(ctx, "req-1", time.Second, 10); err != nil {
		t.Fatalf("req-1 Acquire failed: %v", err)
	}
	if err := q.Acquire(ctx, "req-2", time.Second, 10); err != nil {
		t.Fatalf("req-2 Acquire failed: %v", err)
	}

	if stat.WindowRequests.Load() != 2 {
		t.Errorf("expected WindowRequests=2, got %d", stat.WindowRequests.Load())
	}

	// Release both in-flight requests. Concurrency is now 0/5 (available), but window quota is 2/2 (exhausted)!
	q.Release("req-1")
	q.Release("req-2")

	if q.Capacity() != 5 || q.inFlight != 0 {
		t.Fatalf("expected inFlight=0, capacity=5")
	}

	// 2. Third request arrives: concurrency has slots, BUT window limit is reached!
	// It MUST enter queue and wait.
	req3Acquired := make(chan error, 1)
	go func() {
		req3Acquired <- q.Acquire(ctx, "req-3", 30*time.Minute, 10)
	}()

	// Wait briefly for goroutine to enter queue
	time.Sleep(10 * time.Millisecond)

	if stat.QueueDepth.Load() != 1 {
		t.Fatalf("expected req-3 to be queued (QueueDepth=1), got %d", stat.QueueDepth.Load())
	}

	// 3. Advance time by 5 minutes: window has NOT slid past 10 minutes yet
	q.AdvanceTime(5 * time.Minute)
	time.Sleep(10 * time.Millisecond)

	if stat.QueueDepth.Load() != 1 {
		t.Fatalf("expected req-3 to still be queued after 5m, got %d", stat.QueueDepth.Load())
	}

	// Verify req3 hasn't completed yet
	select {
	case err := <-req3Acquired:
		t.Fatalf("req-3 completed prematurely: %v", err)
	default:
		// Expected: still waiting
	}

	// 4. Advance time by another 5 minutes and 1 millisecond (total > 10m):
	// Oldest requests expire, quota is freed, req-3 should be dispatched immediately!
	q.AdvanceTime(5*time.Minute + time.Millisecond)

	select {
	case err := <-req3Acquired:
		if err != nil {
			t.Fatalf("req-3 Acquire failed after window slid: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for req-3 to be dispatched after window slide")
	}

	if stat.QueueDepth.Load() != 0 {
		t.Errorf("expected QueueDepth=0, got %d", stat.QueueDepth.Load())
	}
	if q.inFlight != 1 {
		t.Errorf("expected inFlight=1, got %d", q.inFlight)
	}

	q.Release("req-3")
}

func TestDynamicQueue_ExpandedQueueCapacity(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("expanded-queue-test")
	q := NewDynamicQueue("expanded-queue-test", 1, stat)

	// Acquire concurrency slot
	if err := q.Acquire(context.Background(), "holder", time.Second, 500); err != nil {
		t.Fatalf("holder acquire failed: %v", err)
	}

	// Queue 100 requests into the expanded queue
	var waiters []chan error
	for i := 0; i < 100; i++ {
		ch := make(chan error, 1)
		waiters = append(waiters, ch)
		go func(idx int) {
			ch <- q.Acquire(context.Background(), "queued", time.Second, 500)
		}(i)
	}

	time.Sleep(20 * time.Millisecond)

	if stat.QueueDepth.Load() != 100 {
		t.Errorf("expected QueueDepth=100, got %d", stat.QueueDepth.Load())
	}
	if stat.RejectedQueueFull.Load() != 0 {
		t.Errorf("expected 0 queue full rejections with expanded queue, got %d", stat.RejectedQueueFull.Load())
	}

	// Release holder, first waiter will acquire
	q.Release("holder")
	time.Sleep(20 * time.Millisecond)

	if stat.QueueDepth.Load() != 99 {
		t.Errorf("expected QueueDepth=99 after dispatch, got %d", stat.QueueDepth.Load())
	}
}

func TestResolveWindowConfig(t *testing.T) {
	cfg := PluginConfig{
		WindowMinutes:     10,
		WindowMaxRequests: 100,
		DefaultRule: ClientTypeRule{
			WindowMinutes:     10,
			WindowMaxRequests: 50,
		},
	}

	// Type with explicit override
	ruleOverride := ClientTypeRule{
		WindowMinutes:     5,
		WindowMaxRequests: 20,
	}
	dur, maxReq := resolveWindowConfig(ruleOverride, cfg)
	if dur != 5*time.Minute || maxReq != 20 {
		t.Errorf("expected 5m / 20, got %v / %d", dur, maxReq)
	}

	// Type inheriting from default_rule
	ruleInherit := ClientTypeRule{}
	dur2, maxReq2 := resolveWindowConfig(ruleInherit, cfg)
	if dur2 != 10*time.Minute || maxReq2 != 100 { // Top-level cfg.WindowMaxRequests has precedence over default_rule
		t.Errorf("expected 10m / 100, got %v / %d", dur2, maxReq2)
	}
}

func TestDynamicQueue_DynamicMaxQueueSizeScaling(t *testing.T) {
	metrics := NewMetricsCollector()
	stat := metrics.ForType("dynamic-max-queue-test")
	q := NewDynamicQueue("dynamic-max-queue-test", 1, stat)

	// Set dynamic max queue size to 2
	q.SetMaxQueueSize(2)
	if got := q.MaxQueueSize(); got != 2 {
		t.Fatalf("expected MaxQueueSize=2, got %d", got)
	}

	ctx := context.Background()
	// Fill concurrency
	_ = q.Acquire(ctx, "req-1", time.Second, 0)

	// Enqueue 2 requests to fill queue
	go func() { _ = q.Acquire(ctx, "req-2", time.Second, 0) }()
	go func() { _ = q.Acquire(ctx, "req-3", time.Second, 0) }()
	time.Sleep(10 * time.Millisecond)

	if stat.QueueDepth.Load() != 2 {
		t.Fatalf("expected QueueDepth=2, got %d", stat.QueueDepth.Load())
	}

	// 4th request should be rejected because queue is full (maxQueueSize=2)
	err := q.Acquire(ctx, "req-4", time.Second, 0)
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Now dynamically scale up queue size (e.g., when new accounts added)
	q.SetMaxQueueSize(5)
	if got := q.MaxQueueSize(); got != 5 {
		t.Fatalf("expected MaxQueueSize=5, got %d", got)
	}

	// Now 4th request can enter queue without error!
	go func() { _ = q.Acquire(ctx, "req-4-retry", time.Second, 0) }()
	time.Sleep(10 * time.Millisecond)

	if stat.QueueDepth.Load() != 3 {
		t.Fatalf("expected QueueDepth=3 after scale up, got %d", stat.QueueDepth.Load())
	}

	q.Release("req-1")
}
