package wal

import (
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Explicit segmented fixture for application-policy integration and legacy comparisons.
func newSegmentedEventAdapter(t *testing.T) (*eventLogAdapter, string) {
	t.Helper()
	path := t.TempDir()
	log, err := segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return &eventLogAdapter{log: log}, path
}
