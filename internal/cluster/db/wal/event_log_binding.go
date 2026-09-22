package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// eventLogBinding identifies one published storage lifetime. Logical operations
// use entries. Native maintenance capabilities are separate: their layouts and
// physical sequences are not part of the entryStore contract. Replacement swaps
// the entire binding under eventMu, invalidating reader source identities.
type eventLogBinding struct {
	entries    entryStore
	compressor eventlog.SealedCompressor
	native     *tidwall.LegacyLog
}

func (b *eventLogBinding) Close() error { return b.entries.Close() }

// bindEventLog includes only background capabilities supplied by the backend.
// A backend that compresses while writing segments has no sealer work.
func bindEventLog(log eventlog.EventLog) *eventLogBinding {
	compressor, _ := log.(eventlog.SealedCompressor)
	return &eventLogBinding{entries: bindEventEntries(log), compressor: compressor}
}
