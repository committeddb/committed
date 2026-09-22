// Package wal contains Committed's production Storage and its shared application
// policies for committed event history: Raft-entry decoding, applied visibility,
// Actual readers, scrub selection, and protected read lifetimes.
//
// The experimental eventLogAdapter applies those policies to the backend-neutral
// eventlog.EventLog interface. Its eventlog_*.go files do not implement physical
// storage or choose a backend. It remains in this package to share production
// policy helpers and error identities; it is not wired into production Storage.
//
// Physical backend implementations live in db/eventlog/tidwall and
// db/eventlog/segmented. The latter delegates to pkg/segmentlog, which owns opaque
// records, immutable segments, the active tail, caching, and atomic publication.
// Production append, lookup, and cursor composition is in legacy_event_*.go;
// experimental backend construction lives in test fixtures. Production lifecycle
// operations retain their native tidwall access.
package wal
