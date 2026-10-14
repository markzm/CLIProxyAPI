package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type authListResponse struct {
	Files []pluginapi.HostAuthFileEntry `json:"files"`
}

// AuthSyncManager handles querying host auth files and calculating dynamic capacities and queue sizes.
type AuthSyncManager struct {
	mu                  sync.RWMutex
	callHostFunc        func(method string, payload any) (json.RawMessage, error)
	lastSyncTime        time.Time
	activeAuthCounts    map[string]int
	effectiveCapacities map[string]int
	effectiveMaxQueues  map[string]int
	effectiveWindowReqs map[string]int
}

// NewAuthSyncManager creates a new AuthSyncManager instance.
func NewAuthSyncManager(callHost func(string, any) (json.RawMessage, error)) *AuthSyncManager {
	return &AuthSyncManager{
		callHostFunc:        callHost,
		activeAuthCounts:    make(map[string]int),
		effectiveCapacities: make(map[string]int),
		effectiveMaxQueues:  make(map[string]int),
		effectiveWindowReqs: make(map[string]int),
	}
}

// SyncActiveAuths queries the host for auth credentials and re-evaluates capacities, queue sizes and window limits.
func (m *AuthSyncManager) SyncActiveAuths(cfg PluginConfig, queues map[string]*DynamicQueue, logFunc func(string, string)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	counts := make(map[string]int)

	if m.callHostFunc != nil {
		rawResp, err := m.callHostFunc(pluginabi.MethodHostAuthList, map[string]any{})
		if err == nil && len(rawResp) > 0 {
			var resp authListResponse
			if errUnmarshal := json.Unmarshal(rawResp, &resp); errUnmarshal == nil {
				for _, file := range resp.Files {
					// Count only active, non-disabled credentials
					if !file.Disabled && file.Status != "unavailable" {
						prov := strings.ToLower(strings.TrimSpace(file.Provider))
						if prov != "" {
							counts[prov]++
						}
					}
				}
			}
		}
	}

	m.activeAuthCounts = counts
	m.lastSyncTime = time.Now()

	// Recalculate capacity, queue size and window limits for each queue
	for clientType, q := range queues {
		rule, ok := cfg.Types[clientType]
		if !ok {
			rule = cfg.DefaultRule
		}

		channel := strings.ToLower(strings.TrimSpace(rule.Channel))
		if channel == "" {
			channel = "default"
		}
		authCount := counts[channel]

		// 1. Concurrency Capacity
		newCap := 1
		if rule.StaticMaxConcurrency > 0 {
			newCap = rule.StaticMaxConcurrency
		} else {
			perAuth := cfg.ChannelPerAuth[channel]
			if perAuth <= 0 {
				perAuth = cfg.DefaultChannelPerAuth
			}
			if perAuth <= 0 {
				perAuth = 2
			}

			calculated := authCount * perAuth
			if calculated > 0 {
				newCap = calculated
			} else {
				newCap = perAuth
			}
		}

		oldCap := q.Capacity()
		if oldCap != newCap {
			q.SetCapacity(newCap)
			if logFunc != nil {
				logFunc("info", fmt.Sprintf("[CAPACITY_RECALCULATED] client_type=%s channel=%s auths=%d per_auth=%d old_cap=%d new_cap=%d",
					clientType, rule.Channel, counts[rule.Channel], cfg.ChannelPerAuth[rule.Channel], oldCap, newCap))
			}
		}
		m.effectiveCapacities[clientType] = newCap

		// 2. Dynamic Max Queue Size (队列长度按账号数量动态计算)
		newMaxQueue := 500
		if rule.StaticMaxQueueSize > 0 {
			newMaxQueue = rule.StaticMaxQueueSize
		} else {
			queuePerAuth := rule.QueuePerAuth
			if queuePerAuth <= 0 {
				queuePerAuth = cfg.ChannelQueuePerAuth[channel]
			}
			if queuePerAuth <= 0 {
				queuePerAuth = cfg.QueuePerAuth[channel]
			}
			if queuePerAuth <= 0 {
				queuePerAuth = cfg.DefaultQueuePerAuth
			}

			if queuePerAuth > 0 {
				if authCount > 0 {
					newMaxQueue = authCount * queuePerAuth
				} else {
					newMaxQueue = queuePerAuth
				}
			} else if rule.MaxQueueSize > 0 {
				newMaxQueue = rule.MaxQueueSize
			} else if cfg.DefaultRule.MaxQueueSize > 0 {
				newMaxQueue = cfg.DefaultRule.MaxQueueSize
			}
		}

		oldMaxQueue := q.MaxQueueSize()
		if oldMaxQueue != newMaxQueue {
			q.SetMaxQueueSize(newMaxQueue)
			if logFunc != nil {
				logFunc("info", fmt.Sprintf("[QUEUE_SIZE_RECALCULATED] client_type=%s channel=%s auths=%d old_queue=%d new_queue=%d",
					clientType, channel, authCount, oldMaxQueue, newMaxQueue))
			}
		}
		m.effectiveMaxQueues[clientType] = newMaxQueue

		// 3. Dynamic Sliding Window Limit (滑动窗口配额按账号数量动态计算)
		windowDuration, oldWindowMax := q.WindowConfig()
		if windowDuration <= 0 {
			windowDuration = time.Duration(rule.WindowMinutes) * time.Minute
			if windowDuration <= 0 {
				windowDuration = time.Duration(cfg.WindowMinutes) * time.Minute
			}
			if windowDuration <= 0 {
				windowDuration = 10 * time.Minute
			}
		}

		newWindowMax := 0
		if rule.StaticWindowMaxRequests > 0 {
			newWindowMax = rule.StaticWindowMaxRequests
		} else {
			windowPerAuth := rule.WindowPerAuth
			if windowPerAuth <= 0 {
				windowPerAuth = cfg.ChannelWindowPerAuth[channel]
			}
			if windowPerAuth <= 0 {
				windowPerAuth = cfg.WindowPerAuth[channel]
			}
			if windowPerAuth <= 0 {
				windowPerAuth = cfg.DefaultWindowPerAuth
			}

			if windowPerAuth > 0 {
				if authCount > 0 {
					newWindowMax = authCount * windowPerAuth
				} else {
					newWindowMax = windowPerAuth
				}
			} else if rule.WindowMaxRequests > 0 {
				newWindowMax = rule.WindowMaxRequests
			} else if cfg.WindowMaxRequests > 0 {
				newWindowMax = cfg.WindowMaxRequests
			} else if cfg.DefaultRule.WindowMaxRequests > 0 {
				newWindowMax = cfg.DefaultRule.WindowMaxRequests
			}
		}

		if oldWindowMax != newWindowMax {
			q.SetWindowConfig(windowDuration, newWindowMax)
			if logFunc != nil {
				logFunc("info", fmt.Sprintf("[WINDOW_LIMIT_RECALCULATED] client_type=%s channel=%s auths=%d old_limit=%d new_limit=%d",
					clientType, channel, authCount, oldWindowMax, newWindowMax))
			}
		}
		m.effectiveWindowReqs[clientType] = newWindowMax
	}
}

// GetSnapshot returns current auth counts, effective capacities, max queues, and window limits.
func (m *AuthSyncManager) GetSnapshot() (counts map[string]int, caps map[string]int, queues map[string]int, windows map[string]int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts = make(map[string]int, len(m.activeAuthCounts))
	for k, v := range m.activeAuthCounts {
		counts[k] = v
	}
	caps = make(map[string]int, len(m.effectiveCapacities))
	for k, v := range m.effectiveCapacities {
		caps[k] = v
	}
	queues = make(map[string]int, len(m.effectiveMaxQueues))
	for k, v := range m.effectiveMaxQueues {
		queues[k] = v
	}
	windows = make(map[string]int, len(m.effectiveWindowReqs))
	for k, v := range m.effectiveWindowReqs {
		windows[k] = v
	}
	return counts, caps, queues, windows
}
