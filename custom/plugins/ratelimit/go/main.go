package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;

static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const (
	pluginName    = "ratelimit"
	pluginVersion = "1.1.0"
)

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	RequestInterceptor     bool `json:"request_interceptor"`
	RequestLifecyclePlugin bool `json:"request_lifecycle_plugin"`
	ManagementAPI          bool `json:"management_api"`
}

type managementRegistration struct {
	Resources []managementResource `json:"resources,omitempty"`
}

type managementResource struct {
	Path        string `json:"Path"`
	Menu        string `json:"Menu"`
	Description string `json:"Description"`
}

type managementRequest struct {
	Method  string      `json:"Method"`
	Path    string      `json:"Path"`
	Headers http.Header `json:"Headers"`
	Query   any         `json:"Query"`
	Body    []byte      `json:"Body"`
}

type managementResponse struct {
	StatusCode int         `json:"StatusCode"`
	Headers    http.Header `json:"Headers"`
	Body       []byte      `json:"Body"`
}

// interceptRequest decodes only the fields needed for admission, so the request body is not
// retained by the plugin while a request is queued.
type interceptRequest struct {
	RequestID    string
	TraceID      string
	SourceFormat string
	Headers      http.Header
}

// runtimeState is an immutable configuration generation. Its queues map is never mutated
// after publication, so request paths read it without locks.
type runtimeState struct {
	cfg    PluginConfig
	queues map[string]*DynamicQueue // configured client types plus "default"
}

func (rs *runtimeState) resolve(claimed string) (string, *DynamicQueue) {
	if q, ok := rs.queues[claimed]; ok {
		return claimed, q
	}
	return defaultTypeName, rs.queues[defaultTypeName]
}

func (rs *runtimeState) ruleFor(clientType string) ClientTypeRule {
	return rs.cfg.RuleFor(clientType)
}

// PluginState maintains runtime state for the ratelimit plugin.
type PluginState struct {
	current atomic.Pointer[runtimeState]

	// reconfigMu serializes configuration installs and policy application. Request admission
	// and completion never take it.
	reconfigMu sync.Mutex
	allQueues  map[string]*DynamicQueue // guarded by reconfigMu; queues are reused across reloads

	classifier   *Classifier
	metrics      *MetricsCollector
	authSync     *AuthSyncManager
	registry     *requestRegistry
	budget       *QueueBudget
	lastSync     atomic.Int64 // unix nanoseconds of the last auth refresh
	pluginErrors atomic.Int64
	logFunc      func(level, message string)
}

var state = newPluginState(callHost, logHost)

func newPluginState(hostCall func(string, any) (json.RawMessage, error), logFunc func(string, string)) *PluginState {
	cfg := DefaultPluginConfig()
	ps := &PluginState{
		allQueues:  make(map[string]*DynamicQueue),
		classifier: NewClassifier(),
		metrics:    NewMetricsCollector(),
		authSync:   NewAuthSyncManager(hostCall),
		registry:   newRequestRegistry(),
		budget:     NewQueueBudget(cfg.MaxTotalWaiters, cfg.MaxQueuedBytes),
		logFunc:    logFunc,
	}
	ps.install(cfg)
	return ps
}

func (s *PluginState) log(level, message string) {
	if s.logFunc != nil {
		s.logFunc(level, message)
	}
}

// install publishes a new configuration generation. Queues of client types that remain
// configured are reused so their waiters and window history survive the reload. Queues of
// removed types are retired: they keep their last policy and drain naturally.
func (s *PluginState) install(cfg PluginConfig) {
	s.reconfigMu.Lock()
	defer s.reconfigMu.Unlock()

	counts := s.authSync.Snapshot().Counts
	names := make([]string, 0, len(cfg.Types)+1)
	for name := range cfg.Types {
		names = append(names, name)
	}
	names = append(names, defaultTypeName)

	queues := make(map[string]*DynamicQueue, len(names))
	for _, name := range names {
		rule := cfg.RuleFor(name)
		policy := EffectivePolicy(cfg, rule, counts[rule.Channel])
		q, ok := s.allQueues[name]
		if ok {
			q.ApplyPolicy(policy)
		} else {
			q = NewDynamicQueue(name, policy, s.budget, s.metrics.ForType(name))
			s.allQueues[name] = q
		}
		queues[name] = q
	}
	s.budget.SetLimits(cfg.MaxTotalWaiters, cfg.MaxQueuedBytes)
	s.current.Store(&runtimeState{cfg: cfg, queues: queues})
}

// applyAuthSnapshot recomputes every active queue's complete policy from one auth snapshot.
func (s *PluginState) applyAuthSnapshot(snap *AuthSnapshot) {
	s.reconfigMu.Lock()
	defer s.reconfigMu.Unlock()
	rs := s.current.Load()
	for name, q := range rs.queues {
		rule := rs.ruleFor(name)
		next := EffectivePolicy(rs.cfg, rule, snap.Counts[rule.Channel])
		prev := q.Policy()
		q.ApplyPolicy(next)
		if prev.Capacity != next.Capacity || prev.MaxQueue != next.MaxQueue || prev.WindowMax != next.WindowMax {
			s.log("info", fmt.Sprintf("[CAPACITY_RECALCULATED] client_type=%s channel=%s auths=%d cap=%d->%d queue=%d->%d window=%d->%d",
				name, next.Channel, next.AuthCount, prev.Capacity, next.Capacity, prev.MaxQueue, next.MaxQueue, prev.WindowMax, next.WindowMax))
		}
	}
}

// syncAuths refreshes credential counts without holding any state lock during the host read.
func (s *PluginState) syncAuths(wait bool) {
	snap, ok := s.authSync.Refresh(wait)
	if !ok {
		return
	}
	s.lastSync.Store(time.Now().UnixNano())
	if snap.Stale {
		s.log("warn", "[AUTH_SYNC_FAILED] keeping last credential snapshot: "+snap.Error)
	}
	s.applyAuthSnapshot(snap)
}

func (s *PluginState) maybeSyncAuths() {
	interval := time.Duration(s.current.Load().cfg.AuthSyncIntervalSeconds) * time.Second
	if time.Since(time.Unix(0, s.lastSync.Load())) < interval {
		return
	}
	s.syncAuths(false)
}

func (s *PluginState) close() {
	s.reconfigMu.Lock()
	defer s.reconfigMu.Unlock()
	for _, q := range s.allQueues {
		q.Close()
	}
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) (code C.int) {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	// A panic escaping into C would abort the whole host process.
	defer func() {
		if recovered := recover(); recovered != nil {
			state.pluginErrors.Add(1)
			writeResponse(response, errorEnvelope("plugin_panic", fmt.Sprint(recovered)))
			code = 1
		}
	}()
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		state.pluginErrors.Add(1)
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	state.close()
}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		return handleReconfigure(request)
	case pluginabi.MethodRequestInterceptBefore:
		return handleInterceptBefore(request)
	case pluginabi.MethodRequestInterceptAfter:
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	case pluginabi.MethodRequestComplete:
		return handleRequestComplete(request)
	case pluginabi.MethodManagementRegister:
		return okEnvelope(managementRegistration{
			Resources: []managementResource{{
				Path:        "/ratelimit/metrics",
				Menu:        "Rate Limit Metrics",
				Description: "Real-time client rate-limiting, queuing and concurrency metrics (requires metrics_token)",
			}},
		})
	case pluginabi.MethodManagementHandle:
		return handleManagement(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginName,
			Version:          pluginVersion,
			Author:           "router-for-me",
			GitHubRepository: "https://github.com/router-for-me/CLIProxyAPI",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "channel_per_auth", Type: pluginapi.ConfigFieldTypeObject, Description: "Per-auth concurrency mapped by channel provider key (e.g. codex: 5, claude: 3)"},
				{Name: "queue_per_auth", Type: pluginapi.ConfigFieldTypeObject, Description: "Per-auth queue depth mapped by channel provider key (e.g. codex: 100)"},
				{Name: "window_per_auth", Type: pluginapi.ConfigFieldTypeObject, Description: "Per-auth sliding window max requests mapped by channel provider key (e.g. codex: 50)"},
				{Name: "types", Type: pluginapi.ConfigFieldTypeObject, Description: "Queue, window and timeout rules mapped by client type; unset fields keep their defaults"},
				{Name: "window_minutes", Type: pluginapi.ConfigFieldTypeInteger, Description: "Sliding window duration in minutes (default: 10)"},
				{Name: "window_max_requests", Type: pluginapi.ConfigFieldTypeInteger, Description: "Max requests admitted within the sliding window before requests queue (0 = disabled)"},
				{Name: "max_total_waiters", Type: pluginapi.ConfigFieldTypeInteger, Description: "Max queued requests across all client types; each holds one OS thread (default: 1000)"},
				{Name: "max_queued_bytes", Type: pluginapi.ConfigFieldTypeInteger, Description: "Max total payload bytes of queued requests (default: 268435456)"},
				{Name: "metrics_token", Type: pluginapi.ConfigFieldTypeString, Description: "Bearer token required by the metrics resource; metrics are disabled when empty"},
			},
		},
		Capabilities: registrationCapability{
			RequestInterceptor:     true,
			RequestLifecyclePlugin: true,
			ManagementAPI:          true,
		},
	}
}

func handleReconfigure(request []byte) ([]byte, error) {
	var req lifecycleRequest
	if len(request) > 0 {
		if err := json.Unmarshal(request, &req); err != nil {
			return nil, fmt.Errorf("decode lifecycle request: %w", err)
		}
	}
	cfg, err := ParseConfig(req.ConfigYAML)
	if err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}
	state.install(cfg)
	state.syncAuths(true)
	if cfg.MetricsToken == "" {
		state.log("info", "[RATELIMIT_CONFIGURED] metrics resource disabled: metrics_token is not set")
	}
	state.log("info", "[RATELIMIT_CONFIGURED] plugin configuration loaded successfully")
	return okEnvelope(pluginRegistration())
}

func passThrough() ([]byte, error) {
	return okEnvelope(pluginapi.RequestInterceptResponse{})
}

func handleInterceptBefore(request []byte) ([]byte, error) {
	var req interceptRequest
	if err := json.Unmarshal(request, &req); err != nil {
		return nil, fmt.Errorf("decode request intercept: %w", err)
	}
	if req.RequestID == "" {
		// Without an ID the slot could never be released; do not limit such requests.
		state.log("warn", "[RATELIMIT_SKIPPED] request without request id")
		return passThrough()
	}

	state.maybeSyncAuths()
	rs := state.current.Load()
	clientType, queue := rs.resolve(state.classifier.Resolve(req.Headers, req.SourceFormat))

	ctx, ok := state.registry.begin(req.RequestID, clientType, queue)
	if !ok {
		// The host already reported this request as finished; nobody is waiting for the answer.
		return passThrough()
	}

	wait, errAcquire := queue.Acquire(ctx, int64(len(request)))
	if errAcquire != nil {
		state.registry.abandon(req.RequestID)
		if errors.Is(errAcquire, context.Canceled) {
			state.log("debug", fmt.Sprintf("[QUEUE_CANCELED] client_type=%s req_id=%s trace_id=%s wait_ms=%d",
				clientType, req.RequestID, req.TraceID, wait.Milliseconds()))
			return passThrough()
		}
		return rejectResponse(queue, clientType, req, wait, errAcquire)
	}

	if !state.registry.admit(req.RequestID) {
		// Completed while acquiring: hand the slot back immediately.
		queue.Release()
		return passThrough()
	}

	if wait > 0 {
		state.log("info", fmt.Sprintf("[QUEUE_ACQUIRED] client_type=%s req_id=%s trace_id=%s wait_ms=%d",
			clientType, req.RequestID, req.TraceID, wait.Milliseconds()))
	}
	return passThrough()
}

func rejectResponse(queue *DynamicQueue, clientType string, req interceptRequest, wait time.Duration, errAcquire error) ([]byte, error) {
	state.log("warn", fmt.Sprintf("[RATELIMIT_REJECTED] reason=%v client_type=%s req_id=%s trace_id=%s wait_ms=%d",
		errAcquire, clientType, req.RequestID, req.TraceID, wait.Milliseconds()))
	errJSON, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": fmt.Sprintf("Too Many Requests: client rate limit exceeded (%v)", errAcquire),
			"type":    "rate_limit_error",
			"code":    "rate_limit_exceeded",
		},
	})
	return okEnvelope(pluginapi.RequestInterceptResponse{
		Terminate:  true,
		StatusCode: http.StatusTooManyRequests,
		ResponseHeaders: http.Header{
			"Content-Type": {"application/json"},
			"Retry-After":  {strconv.Itoa(queue.RetryAfter())},
		},
		ResponseBody: errJSON,
	})
}

func handleRequestComplete(request []byte) ([]byte, error) {
	var comp pluginapi.RequestCompletion
	if err := json.Unmarshal(request, &comp); err != nil {
		return nil, fmt.Errorf("decode request completion: %w", err)
	}
	if comp.RequestID == "" {
		return okEnvelope(map[string]any{"ok": true})
	}
	if queue, clientType := state.registry.complete(comp.RequestID); queue != nil {
		queue.Release()
		state.log("debug", fmt.Sprintf("[CONCURRENCY_RELEASED] client_type=%s req_id=%s outcome=%s duration_ms=%d",
			clientType, comp.RequestID, comp.Outcome, comp.CompletedAt.Sub(comp.StartedAt).Milliseconds()))
	}
	return okEnvelope(map[string]any{"ok": true})
}

func jsonManagementResponse(status int, body any) ([]byte, error) {
	raw, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		raw = []byte(`{"error":"failed to serialize metrics"}`)
		status = http.StatusInternalServerError
	}
	return okEnvelope(managementResponse{
		StatusCode: status,
		Headers:    http.Header{"Content-Type": {"application/json"}},
		Body:       raw,
	})
}

// authorizedMetrics checks the plugin-owned bearer token. The host serves plugin resources
// without management authentication, so the plugin must authenticate them itself.
func authorizedMetrics(token string, headers http.Header) bool {
	if token == "" {
		return false
	}
	value := strings.TrimSpace(headers.Get("Authorization"))
	if len(value) < 7 || !strings.EqualFold(value[:7], "bearer ") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(value[7:])), []byte(token)) == 1
}

func handleManagement(request []byte) ([]byte, error) {
	var req managementRequest
	if len(request) > 0 {
		if err := json.Unmarshal(request, &req); err != nil {
			return nil, fmt.Errorf("decode management request: %w", err)
		}
	}
	rs := state.current.Load()
	if rs.cfg.MetricsToken == "" {
		return jsonManagementResponse(http.StatusForbidden, map[string]string{"error": "metrics disabled: set metrics_token in the ratelimit plugin config"})
	}
	if !authorizedMetrics(rs.cfg.MetricsToken, req.Headers) {
		return jsonManagementResponse(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	}

	state.maybeSyncAuths()
	channels := make(map[string]string, len(rs.queues))
	for name, q := range rs.queues {
		q.RefreshWindowMetrics()
		channels[name] = q.Policy().Channel
	}
	waiters, maxWaiters, bytes, maxBytes := state.budget.Usage()
	pending, admitted, tombstones := state.registry.counts()
	snap := state.authSync.Snapshot()
	return jsonManagementResponse(http.StatusOK, map[string]any{
		"ratelimit_metrics": state.metrics.Snapshot(channels),
		"global": map[string]any{
			"queued_waiters":    waiters,
			"max_total_waiters": maxWaiters,
			"queued_bytes":      bytes,
			"max_queued_bytes":  maxBytes,
			"pending_requests":  pending,
			"admitted_requests": admitted,
			"tombstones":        tombstones,
			"plugin_errors":     state.pluginErrors.Load(),
		},
		"auth_sync": map[string]any{
			"counts":    snap.Counts,
			"synced_at": snap.SyncedAt.UTC().Format(time.RFC3339),
			"stale":     snap.Stale,
			"error":     snap.Error,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func logHost(level, message string) {
	_, _ = callHost(pluginabi.MethodHostLog, map[string]any{
		"level":   level,
		"message": message,
	})
}

func callHost(method string, payload any) (json.RawMessage, error) {
	rawPayload, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return nil, fmt.Errorf("marshal host callback %s: %w", method, errMarshal)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	var response C.cliproxy_buffer
	var requestPtr *C.uint8_t
	if len(rawPayload) > 0 {
		cPayload := C.CBytes(rawPayload)
		if cPayload == nil {
			return nil, fmt.Errorf("allocate host callback %s", method)
		}
		defer C.free(cPayload)
		requestPtr = (*C.uint8_t)(cPayload)
	}

	callCode := C.call_host_api(cMethod, requestPtr, C.size_t(len(rawPayload)), &response)
	var rawResponse []byte
	if response.ptr != nil && response.len > 0 {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(response.ptr, response.len)
	}
	if len(rawResponse) == 0 {
		return nil, fmt.Errorf("host callback %s returned no response, code=%d", method, int(callCode))
	}

	var env envelope
	if errUnmarshal := json.Unmarshal(rawResponse, &env); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host envelope %s: %w", method, errUnmarshal)
	}
	if !env.OK {
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
		}
		return nil, fmt.Errorf("host callback %s failed", method)
	}
	return env.Result, nil
}

func writeResponse(buf *C.cliproxy_buffer, raw []byte) {
	if buf == nil {
		return
	}
	if len(raw) == 0 {
		buf.ptr = nil
		buf.len = 0
		return
	}
	cBytes := C.CBytes(raw)
	buf.ptr = cBytes
	buf.len = C.size_t(len(raw))
}

func okEnvelope(result any) ([]byte, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{
		OK:     true,
		Result: json.RawMessage(raw),
	})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{
		OK: false,
		Error: &envelopeError{
			Code:    code,
			Message: message,
		},
	})
	return raw
}
