package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestPostgresStatusSnapshotPhase covers the offline half of Status: a position
// that still carries snapshot progress reports phase=snapshot, the decoded LSN,
// per-table progress, and NO lag (the lag query is skipped mid-snapshot, so the
// test needs no source). The streaming-phase lag query is covered under docker.
func TestPostgresStatusSnapshotPhase(t *testing.T) {
	d := &PostgreSQLDialect{}
	cfg := &sql.Config{Tables: []string{"region", "nation"}}

	progress := &dialectpb.SnapshotProgress{
		CompletedTables: []string{"region"},
		LastPkByTable:   map[string]string{"nation": "7"},
	}
	pos, err := encodePosition(pglogrepl.LSN(0x1A2B3C8), progress)
	require.NoError(t, err)

	st, err := d.Status(context.Background(), cfg, pos)
	require.NoError(t, err)
	require.Equal(t, "snapshot", st.Phase)
	require.Equal(t, pglogrepl.LSN(0x1A2B3C8).String(), st.Position)
	require.Nil(t, st.Lag, "no lag query during snapshot")
	require.False(t, st.CaughtUp)
	require.Equal(t, []cluster.TableSnapshotStatus{
		{Table: "region", Complete: true},
		{Table: "nation", LastKey: "7"},
	}, st.SnapshotProgress)
}

// TestPostgresStatusBadPosition: an unrecognized checkpoint blob is a hard
// error (a corrupt position should surface, not be silently reported as 0).
func TestPostgresStatusBadPosition(t *testing.T) {
	d := &PostgreSQLDialect{}
	cfg := &sql.Config{Tables: []string{"region"}}
	_, err := d.Status(context.Background(), cfg, cluster.Position([]byte{0x01, 0x02, 0x03}))
	require.Error(t, err)
}
