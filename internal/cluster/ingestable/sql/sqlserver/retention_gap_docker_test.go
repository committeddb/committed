//go:build docker

package sqlserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// TestSQLServerRetentionGapResnapshots drives the minValid > consumed branch
// through the loop (it had no test): a worker resuming from a version the
// source no longer retains change history for must re-snapshot at a bumped
// epoch and close with a refresh-boundary marker. Retention cleanup cannot
// be forced in a test (a 30-minute cadence), so the gap is produced the
// other way a source loses its history: change tracking disabled and
// re-enabled on the table, which moves its min valid version up to the
// re-enable point. The SQL Server twin of TestPostgresSlotRecreatedResnapshots.
func TestSQLServerRetentionGapResnapshots(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_gap`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_gap (pk NVARCHAR(32) NOT NULL PRIMARY KEY, v NVARCHAR(50))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_gap (pk, v) VALUES ('seed', 's')`)
	require.NoError(t, err)

	config := &sql.Config{
		Type:             &cluster.Type{ID: "ct-gap", Name: "ct-gap"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_gap"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	// Run 1: snapshot the seed, reach streaming, capture the checkpoint.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() { _ = (&sqlserver.SQLServerDialect{}).Ingest(ctx1, config, nil, 0, pr1, po1) }()
	checkpoint := awaitStreaming(t, pr1, po1, 2*time.Minute, "seed").Position
	cancel1()
	require.NotEmpty(t, checkpoint)

	// While no worker runs: a tracked change first, so the database's version
	// moves past the checkpoint (a fresh database sits at version 0, and a
	// table re-enabled at the consumed version is valid by SQL Server's own
	// rule — CHANGE_TRACKING_MIN_VALID_VERSION <= last_sync_version — even
	// though history was lost; that is the source's hole, not the dialect's).
	// Then lose the history around a change the checkpoint does not cover:
	// the re-enabled table's min valid version lands at the advanced version,
	// above the consumed one, and the row changed while tracking was off has
	// no change-table entry — only a re-snapshot can deliver it.
	_, err = db.Exec(`INSERT INTO dbo.ct_gap (pk, v) VALUES ('tracked', 't')`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE dbo.ct_gap DISABLE CHANGE_TRACKING`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_gap (pk, v) VALUES ('lost', 'l')`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE dbo.ct_gap ENABLE CHANGE_TRACKING`)
	require.NoError(t, err)

	// Capture the loop's Error lines: the recovery must be the loud retention
	// path, not an incidental cold snapshot.
	core, observed := observer.New(zap.ErrorLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	// Run 2: resume from the stale checkpoint. The re-snapshot's rows arrive
	// first and its closing marker after them, so the marker is counted only
	// once 'lost' has been seen.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	errCh := make(chan error, 1)
	go func() { errCh <- (&sqlserver.SQLServerDialect{}).Ingest(ctx2, config, checkpoint, 0, pr2, po2) }()

	genByKey := map[string]uint64{}
	var sawMarker bool
	var markerEpoch uint64
	for !sawMarker {
		select {
		case p := <-pr2:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					if _, haveLost := genByKey["lost"]; haveLost {
						sawMarker = true
						markerEpoch = e.Generation
					}
					continue
				}
				genByKey[string(e.Key)] = e.Generation
			}
		case <-po2:
		case ingestErr := <-errCh:
			t.Fatalf("Ingest exited instead of recovering from the retention gap: %v", ingestErr)
		case <-ctx2.Done():
			t.Fatal("'lost' + its closing refresh-boundary marker never arrived — a retention gap must re-snapshot and close with a reconciling marker")
		}
	}
	cancel2()

	require.NotEmpty(t, observed.FilterMessageSnippet("retention purged past the consumed version").All(),
		"recovery must take the retention-gap branch")
	require.Equal(t, uint64(2), markerEpoch,
		"gap recovery bumps the epoch once above run 1's generation (1 → 2)")
	for _, k := range []string{"seed", "tracked", "lost"} {
		require.Equal(t, markerEpoch, genByKey[k], "row %q must re-snapshot at the marker's generation", k)
	}
}
