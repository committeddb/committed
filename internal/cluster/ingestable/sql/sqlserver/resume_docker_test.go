//go:build docker

package sqlserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// TestSQLServerResumeFromStreamingCheckpoint pins the third resume state
// (fresh and mid-snapshot are covered by the e2e and hook tests): a worker
// restarted with a streaming checkpoint must NOT re-snapshot — it resumes
// the poll from the consumed version and delivers only changes committed
// after the checkpoint.
func TestSQLServerResumeFromStreamingCheckpoint(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_resume`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_resume (pk INT NOT NULL PRIMARY KEY, v NVARCHAR(50))`)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO dbo.ct_resume (pk, v) VALUES (1, 'pre')")
	require.NoError(t, err)

	typ := &cluster.Type{ID: "ct-resume", Name: "ct-resume"}
	config := &sql.Config{
		Type:             typ,
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_resume"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	// Run 1: snapshot the one row, reach streaming, capture the checkpoint.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	d1 := &sqlserver.SQLServerDialect{}
	go func() { _ = d1.Ingest(ctx1, config, nil, 0, pr1, po1) }()
	checkpoint := awaitStreaming(t, pr1, po1, 2*time.Minute, "1").Position
	cancel1()
	require.NotEmpty(t, checkpoint)

	// Changes AFTER the checkpoint, while no worker runs (the downtime gap).
	_, err = db.Exec("INSERT INTO dbo.ct_resume (pk, v) VALUES (2, 'during-downtime')")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM dbo.ct_resume WHERE pk = 1")
	require.NoError(t, err)

	// Run 2: resume from the streaming checkpoint. No re-snapshot — row 2's
	// insert and row 1's tombstone arrive via the poll, and row 1's original
	// snapshot upsert must NOT re-emit (that would be a re-snapshot).
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	d2 := &sqlserver.SQLServerDialect{}
	go func() { _ = d2.Ingest(ctx2, config, checkpoint, 0, pr2, po2) }()

	got := drainEntities(t, pr2, po2, 2, 2*time.Minute)
	var sawInsert, sawDelete bool
	for _, e := range got {
		switch string(e.Key) {
		case "2":
			require.False(t, e.IsDelete())
			sawInsert = true
		case "1":
			require.True(t, e.IsDelete(),
				"row 1 must arrive as the downtime DELETE's tombstone, not a re-snapshot upsert")
			sawDelete = true
		}
	}
	require.True(t, sawInsert, "downtime insert must be re-delivered on resume")
	require.True(t, sawDelete, "downtime delete must be re-delivered on resume")
}

// TestSQLServerCompositePKAndUniqueidentifier pins two breadth cases from
// the ticket: a composite primary key (keyset pagination's expanded OR form,
// composite entity keys, composite delete tombstones) and the
// uniqueidentifier mixed-endian gotcha (the driver returns 16 raw bytes; the
// dialect must render the canonical GUID string identically on snapshot,
// upsert, and delete paths).
func TestSQLServerCompositePKAndUniqueidentifier(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_comp`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_comp (
		a INT NOT NULL, b NVARCHAR(20) NOT NULL, g UNIQUEIDENTIFIER NOT NULL,
		v NVARCHAR(50), PRIMARY KEY (a, b))`)
	require.NoError(t, err)
	const guid = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
	for i := 1; i <= 3; i++ {
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO dbo.ct_comp (a, b, g, v) VALUES (%d, 'b%d', '%s', 'v%d')", i, i, guid, i))
		require.NoError(t, err)
	}

	typ := &cluster.Type{ID: "ct-comp", Name: "ct-comp"}
	config := &sql.Config{
		Type: typ,
		Mappings: []sql.Mapping{
			{JsonName: "a", SQLColumn: "a"},
			{JsonName: "b", SQLColumn: "b"},
			{JsonName: "g", SQLColumn: "g"},
			{JsonName: "v", SQLColumn: "v"},
		},
		PrimaryKey:       []string{"a", "b"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_comp"},
		// batch_size 2 forces the composite keyset's expanded-OR resume WHERE
		// across batches (3 rows → two batches).
		Options: map[string]string{"poll_interval": "300ms", "batch_size": "2"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	d := &sqlserver.SQLServerDialect{}
	go func() { _ = d.Ingest(ctx, config, nil, 0, pr, po) }()

	snap := drainEntities(t, pr, po, 3, 2*time.Minute)
	byKey := map[string]*cluster.Entity{}
	for _, e := range snap {
		byKey[string(e.Key)] = e
	}
	require.Len(t, byKey, 3, "composite keyset pagination must enumerate every row exactly once")
	require.Contains(t, byKey, `["1","b1"]`, "composite keys are the JSON-array encoding")
	require.JSONEq(t,
		fmt.Sprintf(`{"a":1,"b":"b1","g":"%s","v":"v1"}`, guid),
		string(byKey[`["1","b1"]`].Data),
		"uniqueidentifier must render as the canonical GUID string, not raw bytes")

	// A delete keyed by the composite PK through the CT path.
	_, err = db.Exec("DELETE FROM dbo.ct_comp WHERE a = 2 AND b = 'b2'")
	require.NoError(t, err)
	live := drainEntities(t, pr, po, 1, 2*time.Minute)
	require.True(t, live[0].IsDelete())
	require.Equal(t, `["2","b2"]`, string(live[0].Key),
		"the tombstone's composite key must match the snapshot's key encoding exactly")
}
