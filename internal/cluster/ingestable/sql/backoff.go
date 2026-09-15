package sql

import (
	"context"
	"time"
)

// The reconnect delay bounds every ingest dialect shares: after a failed
// attempt the next one waits ReconnectBackoffMin, doubling per consecutive
// failure up to ReconnectBackoffMax.
const (
	ReconnectBackoffMin = 1 * time.Second
	ReconnectBackoffMax = 30 * time.Second
)

// Backoff is the reconnect delay of one ingest worker session: it climbs
// across consecutive failed attempts and resets once an attempt reaches
// streaming (or completes a snapshot), so a stream that ran healthy for
// hours and then dropped reconnects at the initial delay, not wherever an
// earlier climb left off. One per Ingest call; not safe for concurrent use.
type Backoff struct {
	initial, limit, cur time.Duration
}

// NewReconnectBackoff is the Backoff every dialect's reconnect loop uses.
func NewReconnectBackoff() *Backoff {
	return NewBackoff(ReconnectBackoffMin, ReconnectBackoffMax)
}

// NewBackoff starts at initial and never waits longer than limit.
func NewBackoff(initial, limit time.Duration) *Backoff {
	return &Backoff{initial: initial, limit: limit, cur: initial}
}

// Delay is the delay the next Wait sleeps — for the reconnect log line.
func (b *Backoff) Delay() time.Duration { return b.cur }

// Wait sleeps the current delay, then doubles it (capped at the limit) for
// the failure after this one. It returns ctx.Err() when the context ends
// first, leaving the delay unchanged.
func (b *Backoff) Wait(ctx context.Context) error {
	t := time.NewTimer(b.cur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	b.cur = min(b.cur*2, b.limit)
	return nil
}

// Reset returns the delay to the initial value: the attempt reached
// streaming, so the next failure is a fresh one, not a continuation.
func (b *Backoff) Reset() { b.cur = b.initial }
