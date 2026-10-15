package main

import (
	"context"
	"sync"
	"time"
)

const (
	tombstoneTTL  = 2 * time.Minute
	maxTombstones = 100_000
)

type requestEntry struct {
	clientType string
	queue      *DynamicQueue
	admitted   bool
	cancel     context.CancelFunc
}

type tombstone struct {
	id      string
	expires time.Time
}

// requestRegistry tracks every request from the moment it enters the interceptor until its
// terminal request.complete event.
//
// The C ABI cannot carry host cancellation into a blocking plugin call. However, the host only
// emits request.complete after the interceptor call returns from the host's point of view, so a
// completion for a request that is still pending here means the caller abandoned it (typically a
// client disconnect). The registry turns that completion into cancellation of the queued wait.
// Completions that arrive before the request is registered are remembered as tombstones.
type requestRegistry struct {
	mu         sync.Mutex
	entries    map[string]*requestEntry
	tombstones map[string]time.Time
	order      []tombstone
	nowFunc    func() time.Time
}

func newRequestRegistry() *requestRegistry {
	return &requestRegistry{
		entries:    make(map[string]*requestEntry),
		tombstones: make(map[string]time.Time),
		nowFunc:    time.Now,
	}
}

// begin registers a pending request. It returns ok=false when the request already completed
// (tombstoned) or is already registered, in which case the caller must not limit it.
func (r *requestRegistry) begin(id, clientType string, q *DynamicQueue) (context.Context, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked()
	if _, done := r.tombstones[id]; done {
		delete(r.tombstones, id)
		return nil, false
	}
	if _, exists := r.entries[id]; exists {
		return nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.entries[id] = &requestEntry{clientType: clientType, queue: q, cancel: cancel}
	return ctx, true
}

// admit marks a pending request as holding a slot. It returns false when the request completed
// while acquiring; the caller then owns the slot and must release it.
func (r *requestRegistry) admit(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[id]
	if !ok {
		return false
	}
	entry.admitted = true
	return true
}

// abandon removes a pending request that was rejected or canceled without holding a slot.
func (r *requestRegistry) abandon(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[id]; ok && !entry.admitted {
		entry.cancel()
		delete(r.entries, id)
	}
}

// complete handles a terminal event. It returns the queue whose slot must be released, if any.
func (r *requestRegistry) complete(id string) (*DynamicQueue, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[id]
	if !ok {
		r.addTombstoneLocked(id)
		return nil, ""
	}
	delete(r.entries, id)
	entry.cancel()
	if entry.admitted {
		return entry.queue, entry.clientType
	}
	// Still pending: cancellation wakes the waiter, which removes itself from the queue.
	return nil, entry.clientType
}

func (r *requestRegistry) counts() (pending, admitted, tombstones int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.entries {
		if entry.admitted {
			admitted++
		} else {
			pending++
		}
	}
	return pending, admitted, len(r.tombstones)
}

func (r *requestRegistry) addTombstoneLocked(id string) {
	r.pruneLocked()
	if len(r.order) >= maxTombstones {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.tombstones, oldest.id)
	}
	expires := r.nowFunc().Add(tombstoneTTL)
	r.tombstones[id] = expires
	r.order = append(r.order, tombstone{id: id, expires: expires})
}

func (r *requestRegistry) pruneLocked() {
	now := r.nowFunc()
	idx := 0
	for idx < len(r.order) && !r.order[idx].expires.After(now) {
		item := r.order[idx]
		if expires, ok := r.tombstones[item.id]; ok && expires.Equal(item.expires) {
			delete(r.tombstones, item.id)
		}
		idx++
	}
	if idx > 0 {
		r.order = append(r.order[:0], r.order[idx:]...)
	}
}
