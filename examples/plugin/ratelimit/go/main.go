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
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const (
	pluginName    = "ratelimit"
	pluginVersion = "1.0.0"
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

// PluginState maintains runtime state for the ratelimit plugin.
type PluginState struct {
	mu           sync.RWMutex
	config       PluginConfig
	classifier   *Classifier
	metrics      *MetricsCollector
	authSync     *AuthSyncManager
	queues       map[string]*DynamicQueue
	inFlightReqs map[string]string // requestID -> clientType
	lastSync     time.Time
}

var state = newPluginState()

func newPluginState() *PluginState {
	cfg := DefaultPluginConfig()
	metrics := NewMetricsCollector()
	classifier := NewClassifier()

	ps := &PluginState{
		config:       cfg,
		classifier:   classifier,
		metrics:      metrics,
		queues:       make(map[string]*DynamicQueue),
		inFlightReqs: make(map[string]string),
	}

	ps.authSync = NewAuthSyncManager(callHost)

	// Initialize queues from config
	ps.initQueues(cfg)
	return ps
}

func resolveWindowConfig(rule ClientTypeRule, cfg PluginConfig) (time.Duration, int) {
	windowMinutes := rule.WindowMinutes
	if windowMinutes <= 0 {
		windowMinutes = cfg.WindowMinutes
	}
	if windowMinutes <= 0 {
		windowMinutes = 10
	}

	maxReqs := rule.WindowMaxRequests
	if maxReqs <= 0 && cfg.WindowMaxRequests > 0 {
		maxReqs = cfg.WindowMaxRequests
	}
	if maxReqs <= 0 && cfg.DefaultRule.WindowMaxRequests > 0 {
		maxReqs = cfg.DefaultRule.WindowMaxRequests
	}

	if maxReqs > 0 {
		return time.Duration(windowMinutes) * time.Minute, maxReqs
	}
	return 0, 0
}

func (s *PluginState) initQueues(cfg PluginConfig) {
	for clientType, rule := range cfg.Types {
		stat := s.metrics.ForType(clientType)
		q, exists := s.queues[clientType]
		if !exists {
			q = NewDynamicQueue(clientType, 5, stat)
			s.queues[clientType] = q
		}
		maxQueue := rule.MaxQueueSize
		if maxQueue <= 0 {
			maxQueue = 500
		}
		q.SetMaxQueueSize(maxQueue)
		windowSize, maxReqs := resolveWindowConfig(rule, cfg)
		q.SetWindowConfig(windowSize, maxReqs)
	}

	defaultRule := cfg.DefaultRule
	stat := s.metrics.ForType("default")
	q, exists := s.queues["default"]
	if !exists {
		q = NewDynamicQueue("default", 5, stat)
		s.queues["default"] = q
	}
	defaultMaxQueue := defaultRule.MaxQueueSize
	if defaultMaxQueue <= 0 {
		defaultMaxQueue = 500
	}
	q.SetMaxQueueSize(defaultMaxQueue)
	windowSize, maxReqs := resolveWindowConfig(defaultRule, cfg)
	q.SetWindowConfig(windowSize, maxReqs)
}

func (s *PluginState) refreshAllWindowMetrics() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, q := range s.queues {
		q.RefreshWindowMetrics()
	}
}

func (s *PluginState) getQueue(clientType string) *DynamicQueue {
	s.mu.RLock()
	q, ok := s.queues[clientType]
	s.mu.RUnlock()
	if ok {
		return q
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok = s.queues[clientType]; ok {
		return q
	}
	stat := s.metrics.ForType(clientType)
	q = NewDynamicQueue(clientType, 5, stat)
	rule, okRule := s.config.Types[clientType]
	if !okRule {
		rule = s.config.DefaultRule
	}
	maxQueue := rule.MaxQueueSize
	if maxQueue <= 0 {
		maxQueue = 500
	}
	q.SetMaxQueueSize(maxQueue)
	windowSize, maxReqs := resolveWindowConfig(rule, s.config)
	q.SetWindowConfig(windowSize, maxReqs)
	s.queues[clientType] = q
	return q
}

func (s *PluginState) checkAndSyncAuths() {
	s.mu.RLock()
	interval := time.Duration(s.config.AuthSyncIntervalSeconds) * time.Second
	needsSync := time.Since(s.lastSync) >= interval
	s.mu.RUnlock()

	if needsSync {
		s.mu.Lock()
		if time.Since(s.lastSync) >= interval {
			s.lastSync = time.Now()
			s.authSync.SyncActiveAuths(s.config, s.queues, logHost)
		}
		s.mu.Unlock()
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
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
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
func cliproxyPluginShutdown() {}

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
				Description: "Real-time client rate-limiting, queuing and concurrency metrics",
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
			Logo:             "",
			ConfigFields: []pluginapi.ConfigField{
				{
					Name:        "channel_per_auth",
					Type:        pluginapi.ConfigFieldTypeObject,
					Description: "Per-auth concurrency limit mapped by channel provider key (e.g. codex: 5, claude: 3)",
				},
				{
					Name:        "queue_per_auth",
					Type:        pluginapi.ConfigFieldTypeObject,
					Description: "Per-auth queue depth limit mapped by channel provider key (e.g. codex: 100, claude: 100)",
				},
				{
					Name:        "window_per_auth",
					Type:        pluginapi.ConfigFieldTypeObject,
					Description: "Per-auth sliding window max requests limit mapped by channel provider key (e.g. codex: 50)",
				},
				{
					Name:        "types",
					Type:        pluginapi.ConfigFieldTypeObject,
					Description: "Queue, window and timeout rules mapped by client type",
				},
				{
					Name:        "window_minutes",
					Type:        pluginapi.ConfigFieldTypeInteger,
					Description: "Sliding window rate limit duration in minutes (default: 10)",
				},
				{
					Name:        "window_max_requests",
					Type:        pluginapi.ConfigFieldTypeInteger,
					Description: "Max requests admitted within the sliding window before subsequent requests enter queue (0 = disabled)",
				},
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

	state.mu.Lock()
	state.config = cfg
	state.initQueues(cfg)
	state.mu.Unlock()

	state.authSync.SyncActiveAuths(cfg, state.queues, logHost)
	logHost("info", "[RATELIMIT_CONFIGURED] plugin configuration loaded successfully")
	return okEnvelope(pluginRegistration())
}

func handleInterceptBefore(request []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if err := json.Unmarshal(request, &req); err != nil {
		return nil, fmt.Errorf("decode request intercept: %w", err)
	}

	state.checkAndSyncAuths()

	// 1. Resolve client type
	clientType := state.classifier.Resolve(req.Headers, req.SourceFormat, "")

	// 2. Fetch rule and queue
	state.mu.RLock()
	rule, ok := state.config.Types[clientType]
	if !ok {
		rule = state.config.DefaultRule
	}
	state.mu.RUnlock()

	queue := state.getQueue(clientType)
	timeout := time.Duration(rule.QueueTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 600 * time.Second
	}
	maxQueue := queue.MaxQueueSize()
	if maxQueue <= 0 {
		maxQueue = rule.MaxQueueSize
	}
	if maxQueue <= 0 {
		maxQueue = 500
	}

	windowDuration, maxWindowReqs := queue.WindowConfig()
	logHost("debug", fmt.Sprintf("[QUEUE_ENTER] client_type=%s req_id=%s trace_id=%s max_queue=%d timeout_s=%d window_limit=%d window_s=%0.0f",
		clientType, req.RequestID, req.TraceID, maxQueue, int(timeout.Seconds()), maxWindowReqs, windowDuration.Seconds()))

	// 3. Attempt acquisition with queueing
	ctx := context.Background()
	startWait := time.Now()
	errAcquire := queue.Acquire(ctx, req.RequestID, timeout, maxQueue)
	waitMs := time.Since(startWait).Milliseconds()

	if errAcquire != nil {
		// Rate limited / rejected
		logHost("warn", fmt.Sprintf("[RATELIMIT_REJECTED] reason=%v client_type=%s req_id=%s trace_id=%s wait_ms=%d",
			errAcquire, clientType, req.RequestID, req.TraceID, waitMs))

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
				"Retry-After":  {"5"},
			},
			ResponseBody: errJSON,
		})
	}

	// Admitted! Record in-flight map and wait metrics
	state.mu.Lock()
	state.inFlightReqs[req.RequestID] = clientType
	state.mu.Unlock()

	state.metrics.RecordWait(clientType, waitMs)
	logHost("info", fmt.Sprintf("[QUEUE_ACQUIRED] client_type=%s req_id=%s trace_id=%s wait_ms=%d",
		clientType, req.RequestID, req.TraceID, waitMs))

	return okEnvelope(pluginapi.RequestInterceptResponse{Terminate: false})
}

func handleRequestComplete(request []byte) ([]byte, error) {
	var comp pluginapi.RequestCompletion
	if err := json.Unmarshal(request, &comp); err != nil {
		return nil, fmt.Errorf("decode request completion: %w", err)
	}

	state.mu.Lock()
	clientType, exists := state.inFlightReqs[comp.RequestID]
	if exists {
		delete(state.inFlightReqs, comp.RequestID)
	}
	state.mu.Unlock()

	if exists {
		if queue := state.getQueue(clientType); queue != nil {
			queue.Release(comp.RequestID)
			durationMs := comp.CompletedAt.Sub(comp.StartedAt).Milliseconds()
			logHost("debug", fmt.Sprintf("[CONCURRENCY_RELEASED] client_type=%s req_id=%s outcome=%s duration_ms=%d",
				clientType, comp.RequestID, comp.Outcome, durationMs))
		}
	}
	return okEnvelope(map[string]any{"ok": true})
}

func handleManagement(request []byte) ([]byte, error) {
	state.checkAndSyncAuths()
	state.refreshAllWindowMetrics()

	var req managementRequest
	if len(request) > 0 {
		_ = json.Unmarshal(request, &req)
	}

	authCounts, capacities, maxQueues, _ := state.authSync.GetSnapshot()
	snapshot := state.metrics.Snapshot(capacities, authCounts, maxQueues)

	bodyBytes, errMarshal := json.MarshalIndent(map[string]any{
		"ratelimit_metrics": snapshot,
		"timestamp":         time.Now().UTC().Format(time.RFC3339),
	}, "", "  ")
	if errMarshal != nil {
		bodyBytes = []byte(`{"error":"failed to serialize metrics"}`)
	}

	resp := managementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: bodyBytes,
	}
	return okEnvelope(resp)
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
