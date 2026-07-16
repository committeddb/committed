//go:build docker || integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// TestPostgresPublicationReconciledOnAddedTable is the
// postgres-cdc-publication-not-reconciled-silent-loss regression.
// ensurePublication created the publication FOR the initial tables and, when it
// already existed, returned without touching it. So a re-POST that added a table
// to sql.tables left the publication (and, because the slot already existed, the
// snapshot) untouched — pgoutput never streamed the new table and its existing
// rows were silently lost. The fix ALTER-PUBLICATION-ADDs the table and
// backfills its existing rows on the resumed slot.
func TestPostgresPublicationReconciledOnAddedTable(t *testing.T) {
	const tableA, tableB = "recon_a", "recon_b"

	db := createDB(t)
	for _, tbl := range []string{tableA, tableB} {
		_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tbl))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, tbl))
		require.NoError(t, err)
	}
	// tableB already holds a row BEFORE it is ever watched — this is what must be
	// backfilled once it is added to the config.
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('b_existing', 'v')`, tableB))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_recon", "pub_recon")

	newConfig := func(tables []string) *sql.Config {
		return &sql.Config{
			Type:             &cluster.Type{ID: "recon", Name: "recon"},
			Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
			PrimaryKey:       []string{"pk"},
			ConnectionString: connString,
			Tables:           tables,
			Options:          map[string]string{"slot_name": "slot_recon", "publication": "pub_recon"},
		}
	}

	// --- Phase 1: watch only tableA; capture a per-commit resume position. ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 10)
	po1 := make(chan cluster.Position, 10)
	go func() {
		_ = (&postgres.PostgreSQLDialect{}).Ingest(ctx1, newConfig([]string{tableA}), nil, 0, pr1, po1)
	}()

	waitForSlot(t, "slot_recon")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('a1', 'v')`, tableA))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(20 * time.Second)
	seen := map[string]bool{}
	var lastPos cluster.Position
	for !seen["a1"] || lastPos == nil {
		select {
		case p := <-pr1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case pos := <-po1:
			if isCommitPosition(t, pos) {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("phase 1: timed out waiting for tableA proposal + commit position")
		}
	}
	cancel1()
	require.NotEmpty(t, lastPos)

	// --- Phase 2: re-POST adds tableB. Its pre-existing row must be backfilled
	// even though the slot already exists (no full snapshot runs). ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 10)
	po2 := make(chan cluster.Position, 10)
	go func() {
		_ = (&postgres.PostgreSQLDialect{}).Ingest(ctx2, newConfig([]string{tableA, tableB}), lastPos, 0, pr2, po2)
	}()

	deadline = time.After(20 * time.Second)
	seen2 := map[string]bool{}
	for !seen2["b_existing"] {
		select {
		case p := <-pr2:
			for _, e := range p.Entities {
				seen2[string(e.Key)] = true
			}
		case <-po2:
		case <-deadline:
			t.Fatal("phase 2: tableB's pre-existing row was never backfilled — publication not reconciled")
		}
	}
}
