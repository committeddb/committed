//go:build docker

package sqlserver_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// collectProposals gathers proposals until every wanted key has arrived,
// returning each key's carrying proposal (unlike drainEntities, which drops
// the proposal context this test asserts on).
func collectProposals(t *testing.T, pr <-chan *cluster.Proposal, po <-chan cluster.Position, want []string, deadline time.Duration) map[string]*cluster.Proposal {
	t.Helper()
	wanted := map[string]bool{}
	for _, k := range want {
		wanted[k] = true
	}
	got := map[string]*cluster.Proposal{}
	timeout := time.After(deadline)
	for len(got) < len(wanted) {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					continue
				}
				if wanted[string(e.Key)] {
					got[string(e.Key)] = p
				}
			}
		case <-po:
		case <-timeout:
			t.Fatalf("timed out with %d of %d wanted rows", len(got), len(wanted))
		}
	}
	return got
}

// TestSQLServerCaptureProvenance proves the change-tracking poll's
// BEST-EFFORT provenance: a batch spanning exactly one source transaction
// carries that transaction's SYS_CHANGE_VERSION as SourceTxnID (two rows
// changed together share it) plus the sys.dm_tran_commit_table commit time,
// distinct transactions polled separately carry distinct identities, and
// snapshot-phase proposals carry the zero values.
func TestSQLServerCaptureProvenance(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.prov_ct`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.prov_ct (pk INT NOT NULL PRIMARY KEY, val NVARCHAR(100))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.prov_ct (pk, val) VALUES (1, 'sentinel')`)
	require.NoError(t, err)

	typ := &cluster.Type{ID: "prov-ct", Name: "prov-ct"}
	config := &sql.Config{
		Type: typ,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"prov_ct"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 256)
	po := make(chan cluster.Position, 256)
	d := &sqlserver.SQLServerDialect{}
	ingestDone := make(chan struct{})
	go func() { defer close(ingestDone); _ = d.Ingest(ctx, config, nil, 0, pr, po) }()
	// Join the worker on exit so the test never returns while Ingest is still
	// alive (a straggler would race later tests' use of the shared container).
	defer func() {
		cancel()
		select {
		case <-ingestDone:
		case <-time.After(10 * time.Second):
			t.Error("Ingest did not exit after cancel")
		}
	}()

	// Snapshot phase: the sentinel arrives with ZERO provenance.
	snap := collectProposals(t, pr, po, []string{"1"}, 2*time.Minute)
	require.Zero(t, snap["1"].SourceCommitUnixNano, "snapshot proposals carry no source commit time")
	require.Empty(t, snap["1"].SourceTxnID, "snapshot proposals carry no source transaction id")

	// Transaction 1: two rows changed together. Wait for them to arrive
	// BEFORE committing transaction 2, so each transaction lands in its own
	// poll window (a window spanning both would honestly omit provenance —
	// the best-effort contract).
	before := time.Now()
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO dbo.prov_ct (pk, val) VALUES (2, 'a')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO dbo.prov_ct (pk, val) VALUES (3, 'b')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	txn1 := collectProposals(t, pr, po, []string{"2", "3"}, 2*time.Minute)

	_, err = db.Exec(`INSERT INTO dbo.prov_ct (pk, val) VALUES (4, 'c')`)
	require.NoError(t, err)
	after := time.Now()
	txn2 := collectProposals(t, pr, po, []string{"4"}, 2*time.Minute)

	// Transaction identity: the change version, shared by co-transaction
	// rows, distinct across transactions.
	require.NotEmpty(t, txn1["2"].SourceTxnID)
	_, err = strconv.ParseInt(txn1["2"].SourceTxnID, 10, 64)
	require.NoError(t, err, "a change-tracking transaction identity is the decimal SYS_CHANGE_VERSION")
	require.Equal(t, txn1["2"].SourceTxnID, txn1["3"].SourceTxnID,
		"rows changed in one source transaction share a SourceTxnID")
	require.NotEmpty(t, txn2["4"].SourceTxnID)
	require.NotEqual(t, txn1["2"].SourceTxnID, txn2["4"].SourceTxnID,
		"distinct source transactions carry distinct identities")

	// Commit time from sys.dm_tran_commit_table (sa can read it here). The
	// bracket is deliberately wide: datetime carries ~3ms precision but the
	// point is present-and-sane, not clock agreement between host and the
	// (emulated, possibly slow) container.
	for k, p := range map[string]*cluster.Proposal{"2": txn1["2"], "4": txn2["4"]} {
		require.NotZero(t, p.SourceCommitUnixNano, "row %s missing commit time", k)
		require.GreaterOrEqual(t, p.SourceCommitUnixNano, before.Add(-10*time.Minute).UnixNano(), "row %s commit time too early", k)
		require.LessOrEqual(t, p.SourceCommitUnixNano, after.Add(10*time.Minute).UnixNano(), "row %s commit time too late", k)
	}
}
