//go:build docker

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// TestPostgresCompositeAndArrayColumnsRoundTrip pins that the rare column
// shapes — a user composite type, an array of it, a text array — snapshot
// through the ::text projection and stream through pgoutput as the SAME
// bytes: PostgreSQL's I/O-conversion cast to text exists for every data type,
// so there is no "un-castable column" that could fail a table's snapshot.
func TestPostgresCompositeAndArrayColumnsRoundTrip(t *testing.T) {
	const table = "pgtest_composite"
	const slot, pub = "slot_composite", "pub_composite"
	config := &sql.Config{
		Type:             &cluster.Type{ID: "pgcomposite", Name: "pgcomposite"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"id"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "addr", SQLColumn: "addr"},
			{JsonName: "addrs", SQLColumn: "addrs"},
			{JsonName: "tags", SQLColumn: "tags"},
		},
		Options: map[string]string{"slot_name": slot, "publication": pub},
	}

	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`DROP TYPE IF EXISTS pgtest_addr`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TYPE pgtest_addr AS (street text, num int)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (id int PRIMARY KEY, addr pgtest_addr, addrs pgtest_addr[], tags text[])`)
	require.NoError(t, err)
	const values = `ROW('Main "St"', 5)::pgtest_addr, ARRAY[ROW('A,B', 1)::pgtest_addr, ROW(NULL, 2)::pgtest_addr], ARRAY['x', 'y z', 'q"uote']`
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES (1, ` + values + `)`)
	require.NoError(t, err)
	cleanReplication(t, slot, pub)
	t.Cleanup(func() { cleanReplication(t, slot, pub) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, 0, pr, po) }()

	find := func(res ingesttest.Result, key string) map[string]any {
		for _, p := range res.Proposals {
			for _, e := range p.Entities {
				if string(e.Key) == key {
					var m map[string]any
					require.NoError(t, json.Unmarshal(e.Data, &m))
					return m
				}
			}
		}
		t.Fatalf("row %q not among the proposals", key)
		return nil
	}
	snap := find(ingesttest.Await(t, pr, po, 30*time.Second, nil, "1"), "1")
	require.Equal(t, `("Main ""St""",5)`, snap["addr"], "a composite value is its PostgreSQL text form")
	require.Equal(t, `{"(\"A,B\",1)","(,2)"}`, snap["addrs"], "an array of composites likewise")
	require.Equal(t, `{x,"y z","q\"uote"}`, snap["tags"], "a text array likewise")

	waitForSlot(t, slot)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES (2, ` + values + `)`)
	require.NoError(t, err)
	live := find(awaitCommit(t, pr, po, 30*time.Second, "2"), "2")
	for _, col := range []string{"addr", "addrs", "tags"} {
		require.Equal(t, snap[col], live[col], "column %s: the stream must spell the value exactly as the snapshot did", col)
	}
}
