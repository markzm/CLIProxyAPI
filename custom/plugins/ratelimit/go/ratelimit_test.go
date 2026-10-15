package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

var baseTime = time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)

func authHost(entries ...pluginapi.HostAuthFileEntry) func(string, any) (json.RawMessage, error) {
	return func(method string, _ any) (json.RawMessage, error) {
		if method != pluginabi.MethodHostAuthList {
			return nil, fmt.Errorf("unexpected method %s", method)
		}
		return json.Marshal(authListResponse{Files: entries})
	}
}

func codexAuths(n int) []pluginapi.HostAuthFileEntry {
	out := make([]pluginapi.HostAuthFileEntry, n)
	for i := range out {
		out[i] = pluginapi.HostAuthFileEntry{Provider: "codex", Status: "active"}
	}
	return out
}

// useState installs a fresh global plugin state configured from yaml for the duration of a test.
func useState(t *testing.T, yaml string, host func(string, any) (json.RawMessage, error)) *PluginState {
	t.Helper()
	previous := state
	state = newPluginState(host, nil)
	raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(yaml)})
	if _, err := handleReconfigure(raw); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		state.close()
		state = previous
	})
	return state
}

func interceptBody(id, clientType string) []byte {
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{
		RequestID: id,
		Headers:   http.Header{"X-Client-Type": {clientType}},
		Body:      []byte(`{"model":"x"}`),
	})
	return raw
}

func intercept(t *testing.T, id, clientType string) pluginapi.RequestInterceptResponse {
	t.Helper()
	raw, err := handleInterceptBefore(interceptBody(id, clientType))
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("bad envelope %s: %v", raw, err)
	}
	var resp pluginapi.RequestInterceptResponse
	if err := json.Unmarshal(env.Result, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func complete(t *testing.T, id string, outcome pluginapi.RequestCompletionOutcome) {
	t.Helper()
	raw, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: id, Outcome: outcome})
	if _, err := handleRequestComplete(raw); err != nil {
		t.Fatal(err)
	}
}

func queueState(q *DynamicQueue) (inFlight, depth int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inFlight, q.waiters.Len()
}

// awaitDepth waits deterministically (no wall-clock sleep) until the queue holds size waiters.
func awaitDepth(t *testing.T, q *DynamicQueue, size int) {
	t.Helper()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	for {
		if _, depth := queueState(q); depth == size {
			return
		}
		select {
		case <-guard.C:
			_, depth := queueState(q)
			t.Fatalf("queue depth = %d, want %d", depth, size)
		default:
			runtime.Gosched()
		}
	}
}

func testQueue(capacity, maxQueue int) *DynamicQueue {
	return NewDynamicQueue("test", QueuePolicy{Capacity: capacity, MaxQueue: maxQueue, Timeout: time.Minute}, nil, nil)
}

func TestClassifier(t *testing.T) {
	c := NewClassifier()
	tests := []struct {
		name         string
		headers      http.Header
		sourceFormat string
		expected     string
	}{
		{"explicit header wins", http.Header{"X-Client-Type": {"Custom-Worker"}, "User-Agent": {"Codex Desktop/1.0"}}, "", "custom-worker"},
		{"codex desktop", http.Header{"User-Agent": {"Codex Desktop/0.144.0"}}, "", "codex"},
		{"codex cli", http.Header{"User-Agent": {"codex_cli_rs/0.150.0"}}, "", "codex"},
		{"claude code", http.Header{"User-Agent": {"Claude Code/1.0.0"}}, "", "claude-code"},
		{"antigravity", http.Header{"User-Agent": {"antigravity-cli/2.0"}}, "", "gemini"},
		{"cursor", http.Header{"User-Agent": {"Cursor/0.40.0"}}, "", "ide-plugin"},
		{"python bot", http.Header{"User-Agent": {"python-requests/2.31.0"}}, "", "script-bot"},
		{"responses format fallback", http.Header{"User-Agent": {"unknown-client"}}, "openai-response", "codex"},
		{"claude format fallback", http.Header{}, "claude", "claude-code"},
		{"default", http.Header{"User-Agent": {"unknown"}}, "openai", "default"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.Resolve(tt.headers, tt.sourceFormat); got != tt.expected {
				t.Errorf("Resolve() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestQueue_FIFOAcquireRelease(t *testing.T) {
	q := testQueue(1, 10)
	if _, err := q.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	order := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		go func(n int) {
			if _, err := q.Acquire(context.Background(), 1); err == nil {
				order <- n
			}
		}(i)
		awaitDepth(t, q, i)
	}
	q.Release()
	if got := <-order; got != 1 {
		t.Fatalf("first admitted = %d, want 1", got)
	}
	q.Release()
	if got := <-order; got != 2 {
		t.Fatalf("second admitted = %d, want 2", got)
	}
	q.Release()
	if inFlight, depth := queueState(q); inFlight != 0 || depth != 0 {
		t.Fatalf("inFlight=%d depth=%d, want 0/0", inFlight, depth)
	}
}

func TestQueue_TimeoutReleasesBudget(t *testing.T) {
	budget := NewQueueBudget(10, 1000)
	q := NewDynamicQueue("t", QueuePolicy{Capacity: 1, MaxQueue: 5, Timeout: time.Millisecond}, budget, nil)
	if _, err := q.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Acquire(context.Background(), 100); !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("err = %v, want timeout", err)
	}
	if waiters, _, bytes, _ := budget.Usage(); waiters != 0 || bytes != 0 {
		t.Fatalf("budget leaked: waiters=%d bytes=%d", waiters, bytes)
	}
	if got := q.metrics.RejectedTimeout.Load(); got != 1 {
		t.Fatalf("RejectedTimeout = %d", got)
	}
}

func TestQueue_PerQueueAndGlobalLimits(t *testing.T) {
	budget := NewQueueBudget(1, 1<<20)
	a := NewDynamicQueue("a", QueuePolicy{Capacity: 1, MaxQueue: 1, Timeout: time.Minute}, budget, nil)
	b := NewDynamicQueue("b", QueuePolicy{Capacity: 1, MaxQueue: 5, Timeout: time.Minute}, budget, nil)
	_, _ = a.Acquire(context.Background(), 1)
	_, _ = b.Acquire(context.Background(), 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := a.Acquire(ctx, 1)
		done <- err
	}()
	awaitDepth(t, a, 1)
	if _, err := a.Acquire(context.Background(), 1); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("per-queue limit: err = %v", err)
	}
	if _, err := b.Acquire(context.Background(), 1); !errors.Is(err, ErrGlobalQueueFull) {
		t.Fatalf("global limit: err = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter err = %v", err)
	}
	if waiters, _, _, _ := budget.Usage(); waiters != 0 {
		t.Fatalf("budget waiters = %d after cancellation", waiters)
	}
	if got := a.metrics.CanceledInQueue.Load(); got != 1 {
		t.Fatalf("CanceledInQueue = %d", got)
	}
}

func TestQueue_GrantCancelRollbackRemovesOwnWindowEntry(t *testing.T) {
	q := NewDynamicQueue("t", QueuePolicy{Capacity: 1, MaxQueue: 5, Timeout: time.Minute, WindowSize: time.Hour, WindowMax: 10}, nil, nil)
	q.SetMockTime(baseTime)
	_, _ = q.Acquire(context.Background(), 1) // admitted at t0

	w := &waiter{ch: make(chan struct{}, 1)}
	q.mu.Lock()
	w.elem = q.waiters.PushBack(w)
	q.mu.Unlock()

	q.AdvanceTime(time.Minute)
	q.Release() // grants w at t0+1m
	q.ApplyPolicy(QueuePolicy{Capacity: 2, MaxQueue: 5, Timeout: time.Minute, WindowSize: time.Hour, WindowMax: 10})
	q.AdvanceTime(time.Minute)
	_, _ = q.Acquire(context.Background(), 1) // admitted at t0+2m

	q.mu.Lock()
	q.rollbackLocked(w)
	got := append([]time.Time(nil), q.admittedTimes...)
	inFlight := q.inFlight
	q.mu.Unlock()
	if inFlight != 1 || len(got) != 2 || !got[0].Equal(baseTime) || !got[1].Equal(baseTime.Add(2*time.Minute)) {
		t.Fatalf("rollback removed wrong entry: inFlight=%d window=%v", inFlight, got)
	}
}

func TestQueue_SlidingWindow(t *testing.T) {
	q := NewDynamicQueue("t", QueuePolicy{Capacity: 10, MaxQueue: 5, Timeout: time.Minute, WindowSize: 10 * time.Minute, WindowMax: 2}, nil, nil)
	q.SetMockTime(baseTime)
	for i := 0; i < 2; i++ {
		if _, err := q.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	admitted := make(chan struct{})
	go func() {
		if _, err := q.Acquire(context.Background(), 1); err == nil {
			close(admitted)
		}
	}()
	awaitDepth(t, q, 1)
	q.AdvanceTime(5 * time.Minute)
	if _, depth := queueState(q); depth != 1 {
		t.Fatal("admitted before the window expired")
	}
	q.AdvanceTime(5*time.Minute + time.Millisecond)
	<-admitted
}

func TestQueue_ApplyPolicyIsAtomic(t *testing.T) {
	q := NewDynamicQueue("t", QueuePolicy{Capacity: 1, MaxQueue: 10, Timeout: time.Minute, WindowSize: 10 * time.Minute, WindowMax: 100}, nil, nil)
	q.SetMockTime(baseTime)
	_, _ = q.Acquire(context.Background(), 1)
	q.mu.Lock()
	for i := 0; i < 3; i++ {
		w := &waiter{ch: make(chan struct{}, 1)}
		w.elem = q.waiters.PushBack(w)
	}
	q.mu.Unlock()
	q.ApplyPolicy(QueuePolicy{Capacity: 4, MaxQueue: 10, Timeout: time.Minute, WindowSize: 10 * time.Minute, WindowMax: 1})
	q.mu.Lock()
	got := len(q.admittedTimes)
	q.mu.Unlock()
	if got != 1 {
		t.Fatalf("window requests = %d, want 1 (capacity applied before window limit)", got)
	}
}

func BenchmarkQueueRemoveWaiters(b *testing.B) {
	for _, size := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			for b.Loop() {
				q := testQueue(1, size)
				ws := make([]*waiter, size)
				q.mu.Lock()
				for i := range ws {
					ws[i] = &waiter{ch: make(chan struct{}, 1)}
					ws[i].elem = q.waiters.PushBack(ws[i])
				}
				for _, w := range ws {
					q.waiters.Remove(w.elem)
				}
				q.mu.Unlock()
			}
		})
	}
}

func TestConfig_PartialTypeOverrideKeepsDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte("types:\n  codex:\n    max_queue_size: 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	rule := cfg.Types["codex"]
	if rule.Channel != "codex" || rule.QueueTimeoutSeconds != defaultQueueTimeoutSeconds {
		t.Fatalf("partial override lost defaults: %+v", rule)
	}
	p := EffectivePolicy(cfg, rule, 2)
	if p.Capacity != 10 || p.MaxQueue != 3 {
		t.Fatalf("capacity=%d maxQueue=%d, want 10/3", p.Capacity, p.MaxQueue)
	}
}

func TestConfig_QueuePerAuthAlias(t *testing.T) {
	cfg, err := ParseConfig([]byte("queue_per_auth:\n  codex: 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p := EffectivePolicy(cfg, cfg.Types["codex"], 1); p.MaxQueue != 7 {
		t.Fatalf("queue_per_auth.codex=7, effective queue=%d", p.MaxQueue)
	}
	cfg, _ = ParseConfig([]byte("queue_per_auth:\n  codex: 7\nchannel_queue_per_auth:\n  codex: 9\n"))
	if p := EffectivePolicy(cfg, cfg.Types["codex"], 1); p.MaxQueue != 9 {
		t.Fatalf("channel_queue_per_auth must win, got %d", p.MaxQueue)
	}
}

func TestConfig_GlobalWindowApplies(t *testing.T) {
	cfg, err := ParseConfig([]byte("window_minutes: 5\nwindow_max_requests: 10\n"))
	if err != nil {
		t.Fatal(err)
	}
	p := EffectivePolicy(cfg, cfg.Types["codex"], 1)
	if p.WindowSize != 5*time.Minute || p.WindowMax != 10 {
		t.Fatalf("window=%v max=%d, want 5m/10", p.WindowSize, p.WindowMax)
	}
}

func TestConfig_NewTypeInheritsDefaultRule(t *testing.T) {
	cfg, err := ParseConfig([]byte("default_rule:\n  queue_timeout_seconds: 30\ntypes:\n  Batch:\n    static_max_concurrency: 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	rule, ok := cfg.Types["batch"]
	if !ok || rule.QueueTimeoutSeconds != 30 || rule.StaticMaxConcurrency != 3 || rule.Channel != "default" {
		t.Fatalf("rule = %+v ok=%v", rule, ok)
	}
}

func TestConfig_Validation(t *testing.T) {
	for _, yaml := range []string{
		"types:\n  codex:\n    max_queue_size: -1\n",
		"channel_per_auth:\n  codex: -3\n",
		"max_total_waiters: 0\n",
		"max_queued_bytes: -1\n",
		"types:\n  default:\n    static_max_concurrency: 1\n",
		"window_minutes: 2000000\n",
	} {
		if _, err := ParseConfig([]byte(yaml)); err == nil {
			t.Errorf("expected validation error for %q", yaml)
		}
	}
}

func TestAuthSync_EligibilityAndStaleSnapshot(t *testing.T) {
	fail := false
	m := NewAuthSyncManager(func(string, any) (json.RawMessage, error) {
		if fail {
			return nil, errors.New("boom")
		}
		return json.Marshal(authListResponse{Files: []pluginapi.HostAuthFileEntry{
			{Provider: "codex", Status: "active"},
			{Provider: "codex", Status: "error", Unavailable: true},
			{Provider: "codex", Status: "active", Disabled: true},
			{Provider: "codex", Status: "disabled"},
			{Type: "Claude", Status: "active"},
		}})
	})
	snap, _ := m.Refresh(true)
	if snap.Counts["codex"] != 1 || snap.Counts["claude"] != 1 || snap.Stale {
		t.Fatalf("snapshot = %+v", snap)
	}
	fail = true
	snap, _ = m.Refresh(true)
	if !snap.Stale || snap.Counts["codex"] != 1 {
		t.Fatalf("failed refresh must keep last good counts: %+v", snap)
	}
}

func TestState_CanceledQueuedRequestDoesNotLeak(t *testing.T) {
	s := useState(t, "types:\n  codex:\n    static_max_concurrency: 1\n", authHost(codexAuths(1)...))
	q := s.current.Load().queues["codex"]
	intercept(t, "holder", "codex")

	done := make(chan pluginapi.RequestInterceptResponse, 1)
	go func() { done <- intercept(t, "canceled", "codex") }()
	awaitDepth(t, q, 1)

	// The host stopped waiting (client disconnect) and delivered the terminal event first.
	complete(t, "canceled", pluginapi.RequestCompletionCanceled)
	if resp := <-done; resp.Terminate {
		t.Fatal("canceled request must not produce a response")
	}
	complete(t, "holder", pluginapi.RequestCompletionSucceeded)

	if inFlight, depth := queueState(q); inFlight != 0 || depth != 0 {
		t.Fatalf("inFlight=%d depth=%d, want 0/0", inFlight, depth)
	}
	if pending, admitted, _ := s.registry.counts(); pending != 0 || admitted != 0 {
		t.Fatalf("registry pending=%d admitted=%d", pending, admitted)
	}
	if got := q.metrics.CanceledInQueue.Load(); got != 1 {
		t.Fatalf("CanceledInQueue = %d", got)
	}
}

func TestState_CompletionBeforeRegistration(t *testing.T) {
	s := useState(t, "types:\n  codex:\n    static_max_concurrency: 1\n", authHost())
	complete(t, "early", pluginapi.RequestCompletionCanceled)
	intercept(t, "early", "codex")
	q := s.current.Load().queues["codex"]
	if inFlight, _ := queueState(q); inFlight != 0 {
		t.Fatalf("late interceptor call for a completed request took a slot")
	}
	// The next request with the full capacity must be admitted immediately.
	intercept(t, "next", "codex")
	complete(t, "next", pluginapi.RequestCompletionSucceeded)
}

func TestRegistry_CompletionWhileAcquiringReturnsSlot(t *testing.T) {
	r := newRequestRegistry()
	q := testQueue(1, 1)
	ctx, ok := r.begin("id", "codex", q)
	if !ok {
		t.Fatal("begin failed")
	}
	if released, _ := r.complete("id"); released != nil {
		t.Fatal("pending request must not release a slot")
	}
	if ctx.Err() == nil {
		t.Fatal("completion must cancel the pending wait")
	}
	if r.admit("id") {
		t.Fatal("admit after completion must report the slot as orphaned")
	}
}

func TestRegistry_TombstonesExpire(t *testing.T) {
	r := newRequestRegistry()
	now := baseTime
	r.nowFunc = func() time.Time { return now }
	r.complete("a")
	now = now.Add(tombstoneTTL + time.Second)
	if _, ok := r.begin("a", "codex", nil); !ok {
		t.Fatal("expired tombstone still blocks the request id")
	}
	if _, _, tombstones := r.counts(); tombstones != 0 {
		t.Fatalf("tombstones = %d", tombstones)
	}
}

func TestState_UnknownTypesShareDefaultBucket(t *testing.T) {
	s := useState(t, "", authHost())
	rs := s.current.Load()
	before := len(rs.queues)
	for i := 0; i < 20; i++ {
		name, q := rs.resolve(fmt.Sprintf("unknown-%d", i))
		if name != defaultTypeName || q != rs.queues[defaultTypeName] {
			t.Fatalf("unknown type resolved to %q", name)
		}
	}
	intercept(t, "r1", "made-up")
	complete(t, "r1", pluginapi.RequestCompletionSucceeded)
	if len(s.current.Load().queues) != before || len(s.metrics.metrics) != before {
		t.Fatal("client-supplied names created new queues or metrics")
	}
}

func TestState_CapacityFromAuths(t *testing.T) {
	s := useState(t, "", authHost(append(codexAuths(2), pluginapi.HostAuthFileEntry{Provider: "claude", Status: "active"})...))
	rs := s.current.Load()
	if got := rs.queues["codex"].Policy().Capacity; got != 10 {
		t.Fatalf("codex capacity = %d, want 10", got)
	}
	if got := rs.queues["claude-code"].metrics.ActiveAuthCount.Load(); got != 1 {
		t.Fatalf("claude-code active auth count = %d, want 1", got)
	}
}

func TestState_ReconfigureKeepsWindowAndWaiters(t *testing.T) {
	yaml := "types:\n  codex:\n    static_max_concurrency: 1\n    static_window_max_requests: 1\n"
	s := useState(t, yaml, authHost(codexAuths(1)...))
	q := s.current.Load().queues["codex"]
	q.SetMockTime(baseTime)
	if _, err := q.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	q.Release()
	w := &waiter{ch: make(chan struct{}, 1)}
	q.mu.Lock()
	w.elem = q.waiters.PushBack(w)
	q.mu.Unlock()

	raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(yaml)})
	if _, err := handleReconfigure(raw); err != nil {
		t.Fatal(err)
	}
	if s.current.Load().queues["codex"] != q {
		t.Fatal("reconfigure replaced a queue that has waiters")
	}
	if inFlight, _ := queueState(q); inFlight != 0 || len(w.ch) != 0 {
		t.Fatal("reconfigure admitted a waiter while the window quota is exhausted")
	}
}

func TestState_AuthSyncDoesNotBlockAdmission(t *testing.T) {
	entered := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	s := useState(t, "", authHost())
	s.authSync.callHostFunc = func(string, any) (json.RawMessage, error) {
		once.Do(func() { close(entered) })
		<-proceed
		return json.Marshal(authListResponse{})
	}
	s.lastSync.Store(0)
	syncDone := make(chan struct{})
	go func() {
		s.syncAuths(true)
		close(syncDone)
	}()
	<-entered
	intercept(t, "during-sync", "codex")
	complete(t, "during-sync", pluginapi.RequestCompletionSucceeded)
	close(proceed)
	<-syncDone
}

func TestState_ConcurrentReconfigureAndTraffic(t *testing.T) {
	useState(t, "", authHost(codexAuths(2)...))
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(fmt.Sprintf("types:\n  t%d:\n    static_max_concurrency: 100\n", i%5))})
			_, _ = handleMethod(pluginabi.MethodPluginReconfigure, raw)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := fmt.Sprintf("r%d", i)
			_, _ = handleInterceptBefore(interceptBody(id, fmt.Sprintf("t%d", i%5)))
			raw, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: id})
			_, _ = handleRequestComplete(raw)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			state.syncAuths(false)
		}
	}()
	wg.Wait()
}

func managementCall(t *testing.T, headers http.Header) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(managementRequest{Method: http.MethodGet, Path: "/ratelimit/metrics", Headers: headers})
	out, err := handleManagement(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var resp managementResponse
	_ = json.Unmarshal(env.Result, &resp)
	var body map[string]any
	_ = json.Unmarshal(resp.Body, &body)
	return resp.StatusCode, body
}

func TestManagement_RequiresToken(t *testing.T) {
	useState(t, "", authHost())
	if status, _ := managementCall(t, nil); status != http.StatusForbidden {
		t.Fatalf("no token configured: status = %d, want 403", status)
	}

	useState(t, "metrics_token: s3cret\n", authHost(pluginapi.HostAuthFileEntry{Provider: "claude", Status: "active"}))
	if status, _ := managementCall(t, nil); status != http.StatusUnauthorized {
		t.Fatalf("missing header: status = %d, want 401", status)
	}
	if status, _ := managementCall(t, http.Header{"Authorization": {"Bearer wrong"}}); status != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401", status)
	}
	status, body := managementCall(t, http.Header{"Authorization": {"Bearer s3cret"}})
	if status != http.StatusOK {
		t.Fatalf("valid token: status = %d", status)
	}
	metrics := body["ratelimit_metrics"].(map[string]any)
	claude := metrics["claude-code"].(map[string]any)
	if claude["active_auth_count"].(float64) != 1 || claude["channel"] != "claude" {
		t.Fatalf("claude-code metrics = %v", claude)
	}
}
