package http

import "github.com/committeddb/committed/internal/cluster"

// Config holds the parsed HTTP webhook syncable configuration.
type Config struct {
	Topic     string
	URL       string
	Method    string // "POST" or "PUT"
	TimeoutMs int
	Headers   []Header
	// Checkpoint is the per-syncable checkpoint cadence parsed from the
	// common [syncable] section. Zero fields mean "use the default" (the
	// worker resolves them). An HTTP webhook is non-idempotent, so the
	// default Every=1 is the safe choice; raising it trades duplicate
	// exposure on crash for fewer raft round-trips.
	Checkpoint cluster.CheckpointPolicy
}

// Header is a single HTTP header name-value pair.
type Header struct {
	Name  string
	Value string
}
