package iceberg

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

// ReadRowsForTest scans the sink's table and returns the live rows as
// key → payload — the read-back oracle for the docker lifecycle test.
func (s *Syncable) ReadRowsForTest(t *testing.T) map[string]string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.tbl.Refresh(ctx))
	out := map[string]string{}
	_, records, err := s.tbl.Scan().ToArrowRecords(ctx)
	require.NoError(t, err)
	for rec, rerr := range records {
		require.NoError(t, rerr)
		keys := rec.Column(0).(*array.String)
		payloads := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			out[keys.Value(i)] = payloads.Value(i)
		}
		rec.Release()
	}
	return out
}

// SnapshotCountForTest reports how many snapshots the table carries — the
// idempotent-re-commit assertion's probe.
func (s *Syncable) SnapshotCountForTest(t *testing.T) int {
	t.Helper()
	require.NoError(t, s.tbl.Refresh(context.Background()))
	return len(s.tbl.Metadata().Snapshots())
}

// MetadataLocationForTest exposes the table's current metadata JSON location
// for independent readers (the duckdb oracle).
func (s *Syncable) MetadataLocationForTest(t *testing.T) string {
	t.Helper()
	require.NoError(t, s.tbl.Refresh(context.Background()))
	return s.tbl.MetadataLocation()
}
