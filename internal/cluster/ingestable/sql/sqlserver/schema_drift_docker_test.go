//go:build docker

package sqlserver_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// TestSQLServerPrimaryKeyDrift_ParksInsteadOfCollapsing is the SQL Server twin
// of the MySQL/Postgres PK-drift tests: a primaryKey column renamed at the
// source while no worker runs must make Ingest RETURN the park sentinel on
// resume — not retry the failing CHANGETABLE window forever (invisible to
// status and the parked gauge, loud only in logs), and not collapse rows onto
// one key.
func TestSQLServerPrimaryKeyDrift_ParksInsteadOfCollapsing(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_pkdrift`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_pkdrift (id NVARCHAR(32) NOT NULL PRIMARY KEY, val NVARCHAR(50))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_pkdrift (id, val) VALUES ('seed', 'seedval')`)
	require.NoError(t, err)

	config := &sql.Config{
		Type:             &cluster.Type{ID: "ct-pkdrift", Name: "ct-pkdrift"},
		Mappings:         []sql.Mapping{{JsonName: "id", SQLColumn: "id"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"id"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_pkdrift"},
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

	// While no worker runs: RENAME the primary-key column, then insert a row
	// under the new schema so the resumed poll has a window to read.
	_, err = db.Exec(`EXEC sp_rename 'dbo.ct_pkdrift.id', 'id_renamed', 'COLUMN'`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_pkdrift (id_renamed, val) VALUES ('drifted', 'x')`)
	require.NoError(t, err)

	// Run 2: resume from the streaming checkpoint. The first window's
	// CHANGETABLE join names the vanished key column — Ingest must exit with
	// the sentinel, not retry-spin.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	errCh := make(chan error, 1)
	go func() { errCh <- (&sqlserver.SQLServerDialect{}).Ingest(ctx2, config, checkpoint, 0, pr2, po2) }()
	for {
		select {
		case <-pr2:
		case <-po2:
		case ingestErr := <-errCh:
			require.ErrorIs(t, ingestErr, sql.ErrPrimaryKeyColumnMissing,
				"a renamed primary-key column must park the worker (exit with the sentinel), not retry the failing window forever")
			return
		case <-ctx2.Done():
			t.Fatal("Ingest did not park within the deadline — a renamed PK column must exit, not retry-spin")
		}
	}
}

// TestSQLServerMappedColumnDrift_DivergesButKeepsGoing is the divergence-tier
// sibling: dropping a mapped NON-key column while no worker runs must NOT
// park — rows stay keyed by the intact primary key and the field renders
// null — and must Warn that the sink diverges, once per column per session.
func TestSQLServerMappedColumnDrift_DivergesButKeepsGoing(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_mapdrift`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_mapdrift (id NVARCHAR(32) NOT NULL PRIMARY KEY, val NVARCHAR(50), extra NVARCHAR(50))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_mapdrift (id, val, extra) VALUES ('seed', 'seedval', 'e')`)
	require.NoError(t, err)

	config := &sql.Config{
		Type: &cluster.Type{ID: "ct-mapdrift", Name: "ct-mapdrift"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "val", SQLColumn: "val"},
			{JsonName: "extra", SQLColumn: "extra"},
		},
		PrimaryKey:       []string{"id"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_mapdrift"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() { _ = (&sqlserver.SQLServerDialect{}).Ingest(ctx1, config, nil, 0, pr1, po1) }()
	checkpoint := awaitStreaming(t, pr1, po1, 2*time.Minute, "seed").Position
	cancel1()
	require.NotEmpty(t, checkpoint)

	// While no worker runs: DROP the mapped non-key column, then insert under
	// the new schema.
	_, err = db.Exec(`ALTER TABLE dbo.ct_mapdrift DROP COLUMN extra`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_mapdrift (id, val) VALUES ('drifted', 'x')`)
	require.NoError(t, err)

	// Capture warns emitted during run 2 to assert the divergence warning fires.
	core, observed := observer.New(zap.WarnLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	// Run 2: resume; the drifted insert must come through correctly keyed by
	// the intact primary key, with `extra` null, and Ingest must NOT park.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	errCh := make(chan error, 1)
	go func() { errCh <- (&sqlserver.SQLServerDialect{}).Ingest(ctx2, config, checkpoint, 0, pr2, po2) }()

	awaitRow := func(key string) map[string]any {
		deadline := time.After(90 * time.Second)
		for {
			select {
			case p := <-pr2:
				for _, e := range p.Entities {
					if string(e.Key) != key {
						continue
					}
					var m map[string]any
					require.NoError(t, json.Unmarshal(e.Data, &m))
					return m
				}
			case <-po2:
			case ingestErr := <-errCh:
				t.Fatalf("Ingest parked on a mapped non-key column drop (should only diverge+warn): %v", ingestErr)
			case <-deadline:
				t.Fatalf("timed out waiting for row %q; a mapped-column drop must keep streaming, not park", key)
			}
		}
	}
	drifted := awaitRow("drifted")
	require.Equal(t, "drifted", drifted["id"])
	require.Equal(t, "x", drifted["val"])
	require.Nil(t, drifted["extra"], "the dropped column renders null, not a stale value")

	// A second window on the same session must not warn again (deduped).
	_, err = db.Exec(`INSERT INTO dbo.ct_mapdrift (id, val) VALUES ('again', 'y')`)
	require.NoError(t, err)
	_ = awaitRow("again")
	cancel2()

	warns := observed.FilterMessageSnippet("diverges from the source").All()
	require.NotEmpty(t, warns, "a dropped mapped column must warn that the sink diverges")
	extraWarns := 0
	for _, w := range warns {
		if w.ContextMap()["column"] == "extra" {
			extraWarns++
		}
	}
	require.Equal(t, 1, extraWarns, "the divergence warn must fire exactly once per column per session (deduped)")
}
