package asyncapiclient

import (
	"context"
	"fmt"
	"time"
)

// WebSocketPoolSnapshot is a concurrency-safe diagnostic view used by
// conformance tests. It carries counts only and never exposes connections or
// credential-bearing pool keys.
type WebSocketPoolSnapshot struct {
	Connections    int
	ListenerCounts []int
}

func (e *Engine) WebSocketPoolSnapshot() WebSocketPoolSnapshot {
	if e == nil || e.pool == nil {
		return WebSocketPoolSnapshot{}
	}
	e.pool.mu.Lock()
	connections := make([]*pooledWS, 0, len(e.pool.conns))
	for _, connection := range e.pool.conns {
		connections = append(connections, connection)
	}
	e.pool.mu.Unlock()
	snapshot := WebSocketPoolSnapshot{Connections: len(connections), ListenerCounts: make([]int, len(connections))}
	for index, connection := range connections {
		connection.lmu.Lock()
		snapshot.ListenerCounts[index] = len(connection.listeners)
		connection.lmu.Unlock()
	}
	return snapshot
}

// SetWebSocketIdleTimeoutForTesting changes this engine's idle eviction
// timeout. It is intentionally named and documented as a deterministic test
// seam, not as stable application configuration.
func (e *Engine) SetWebSocketIdleTimeoutForTesting(timeout time.Duration) {
	if e != nil && e.pool != nil {
		e.pool.mu.Lock()
		e.pool.idleTimeout = timeout
		e.pool.mu.Unlock()
	}
}

// SendOnSoleWebSocketForTesting injects a frame through the only pooled
// connection. It exists solely for cancellation/isolation conformance tests.
func (e *Engine) SendOnSoleWebSocketForTesting(ctx context.Context, payload []byte) error {
	if e == nil || e.pool == nil {
		return fmt.Errorf("WebSocket pool is unavailable")
	}
	e.pool.mu.Lock()
	if len(e.pool.conns) != 1 {
		count := len(e.pool.conns)
		e.pool.mu.Unlock()
		return fmt.Errorf("expected one pooled WebSocket connection, found %d", count)
	}
	var connection *pooledWS
	for _, value := range e.pool.conns {
		connection = value
	}
	e.pool.mu.Unlock()
	return connection.send(ctx, payload)
}

// SetWebSocketBackpressureBoundsForTesting installs deterministic global
// receive bounds and returns an idempotent restoration function.
func SetWebSocketBackpressureBoundsForTesting(frames, bytes int) func() {
	previousFrames, previousBytes := maxWSBufferedFrames, maxWSBufferedBytes
	maxWSBufferedFrames, maxWSBufferedBytes = frames, bytes
	var restored bool
	return func() {
		if !restored {
			maxWSBufferedFrames, maxWSBufferedBytes = previousFrames, previousBytes
			restored = true
		}
	}
}
