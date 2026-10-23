// Package wal contains Committed's production Storage and its shared application
// policies for committed event history: Raft-entry decoding, applied visibility,
// Actual readers, scrub selection, and protected read lifetimes.
//
// The experimental eventLogAdapter applies those policies to the backend-neutral
// eventlog.EventLog interface. Its eventlog_*.go files do not implement physical
// storage or choose a backend. It remains in this package to share production
// policy helpers and error identities; it is not wired into production Storage.
//
// The event log's physical backend is db/eventlog/segmented, which delegates to
// pkg/segmentlog: opaque records, immutable segments, the active tail, caching,
// and atomic publication. Production logical operations use entryStore over an
// eventLogBinding. A tidwall event log, written before 0.8.3, is only ever read,
// by the converter (convert.go, tidwall_event_reader.go); the Raft logs remain
// tidwall-backed.
package wal
