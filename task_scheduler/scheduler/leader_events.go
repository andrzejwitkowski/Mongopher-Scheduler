package scheduler

import (
	"context"
	"sync"
)

// LeaderEventType represents different leader election events
type LeaderEventType int

const (
	// LeaderElected indicates a new leader was elected
	LeaderElected LeaderEventType = iota
	// LeaderResigned indicates the current leader resigned
	LeaderResigned
)

// LeaderEventHandler defines the interface for handling leader election events
type LeaderEventHandler interface {
	HandleLeaderEvent(ctx context.Context, event LeaderEventType)
}

// LeaderEventRegistry manages leader event subscriptions
type LeaderEventRegistry struct {
	handlers []LeaderEventHandler
	mu       sync.Mutex
}

// NewLeaderEventRegistry creates a new event registry
func NewLeaderEventRegistry() *LeaderEventRegistry {
	return &LeaderEventRegistry{
		handlers: make([]LeaderEventHandler, 0),
	}
}

// Register adds a new event handler
func (r *LeaderEventRegistry) Register(handler LeaderEventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, handler)
}

// Notify sends an event to all registered handlers
func (r *LeaderEventRegistry) Notify(ctx context.Context, event LeaderEventType) {
	r.mu.Lock()
	handlers := make([]LeaderEventHandler, len(r.handlers))
	copy(handlers, r.handlers)
	r.mu.Unlock()

	for _, handler := range handlers {
		handler.HandleLeaderEvent(ctx, event)
	}
}
