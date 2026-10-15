package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type authListResponse struct {
	Files []pluginapi.HostAuthFileEntry `json:"files"`
}

// AuthSnapshot is an immutable view of eligible credential counts keyed by provider.
type AuthSnapshot struct {
	Counts   map[string]int
	SyncedAt time.Time
	// Stale reports that the latest refresh failed and Counts come from the last good read.
	Stale bool
	Error string
}

// AuthSyncManager reads host credentials and publishes snapshots.
// Host reads never run under any plugin lock, and concurrent refreshes are coalesced.
type AuthSyncManager struct {
	callHostFunc func(method string, payload any) (json.RawMessage, error)
	syncMu       sync.Mutex
	snapshot     atomic.Pointer[AuthSnapshot]
}

// NewAuthSyncManager creates a new AuthSyncManager instance.
func NewAuthSyncManager(callHost func(string, any) (json.RawMessage, error)) *AuthSyncManager {
	m := &AuthSyncManager{callHostFunc: callHost}
	m.snapshot.Store(&AuthSnapshot{Counts: map[string]int{}})
	return m
}

// Snapshot returns the latest published snapshot.
func (m *AuthSyncManager) Snapshot() *AuthSnapshot {
	return m.snapshot.Load()
}

// Refresh reads host credentials and publishes a new snapshot. When wait is false and another
// refresh is already running, it returns (nil, false) immediately instead of queueing behind it.
func (m *AuthSyncManager) Refresh(wait bool) (*AuthSnapshot, bool) {
	if wait {
		m.syncMu.Lock()
	} else if !m.syncMu.TryLock() {
		return nil, false
	}
	defer m.syncMu.Unlock()

	previous := m.snapshot.Load()
	counts, err := m.fetchCounts()
	next := &AuthSnapshot{Counts: counts, SyncedAt: time.Now()}
	if err != nil {
		next = &AuthSnapshot{Counts: previous.Counts, SyncedAt: previous.SyncedAt, Stale: true, Error: err.Error()}
	}
	m.snapshot.Store(next)
	return next, true
}

func (m *AuthSyncManager) fetchCounts() (map[string]int, error) {
	if m.callHostFunc == nil {
		return nil, fmt.Errorf("host callback unavailable")
	}
	raw, err := m.callHostFunc(pluginabi.MethodHostAuthList, map[string]any{})
	if err != nil {
		return nil, err
	}
	var resp authListResponse
	if errUnmarshal := json.Unmarshal(raw, &resp); errUnmarshal != nil {
		return nil, fmt.Errorf("decode auth list: %w", errUnmarshal)
	}
	counts := make(map[string]int)
	for _, file := range resp.Files {
		if !authEligible(file) {
			continue
		}
		provider := normalizeName(file.Provider)
		if provider == "" {
			provider = normalizeName(file.Type)
		}
		if provider != "" {
			counts[provider]++
		}
	}
	return counts, nil
}

// authEligible reports whether a credential can currently serve traffic.
// The host reports unavailability as a boolean and uses status values such as "error"/"disabled".
func authEligible(file pluginapi.HostAuthFileEntry) bool {
	if file.Disabled || file.Unavailable {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(file.Status)) {
	case "error", "disabled":
		return false
	}
	return true
}
