package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// eventLogBinding identifies one published storage lifetime. Logical operations
// use entries. Replacement swaps the entire binding under eventMu, invalidating
// reader source identities.
type eventLogBinding struct {
	backup     eventlog.BackupSource
	entries    entryStore
	records    func() eventlog.Cursor
	managed    eventlog.EventLog
	compressor eventlog.SealedCompressor
}

func (b *eventLogBinding) Close() error { return b.entries.Close() }

// bindEventLog includes only background capabilities supplied by the backend.
// A backend that compresses while writing segments has no sealer work.
func bindEventLog(log eventlog.EventLog) *eventLogBinding {
	compressor, _ := log.(eventlog.SealedCompressor)
	return &eventLogBinding{backup: log, entries: bindEventEntries(log), records: func() eventlog.Cursor { return checkedRecordCursor{log.NewCursor()} }, compressor: compressor, managed: log}
}

// eventLogOpener constructs the event binding after the node's metadata lock is
// acquired.
type eventLogOpener func(string, *metrics.Metrics) (*eventLogBinding, error)
