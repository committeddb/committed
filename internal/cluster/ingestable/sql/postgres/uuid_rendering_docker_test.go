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

// TestPostgresUUIDRendersCanonicalLowercase is the PostgreSQL half of the
// cross-engine UUID contract (SQL Server's uniqueidentifier renders the same
// bytes from feature level 5): a uuid value — however it was written —
// ingests as RFC 4122 lowercase in keys and payloads, on the snapshot and the
// stream alike.
func TestPostgresUUIDRendersCanonicalLowercase(t *testing.T) {
	const table = "pgtest_uuid"
	const slot, pub = "slot_uuid", "pub_uuid"
	const upper = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
	const lower = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	config := &sql.Config{
		Type:             &cluster.Type{ID: "pguuid", Name: "pguuid"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"id"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "ref", SQLColumn: "ref"},
		},
		Options: map[string]string{"slot_name": slot, "publication": pub},
	}

	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (id uuid PRIMARY KEY, ref uuid)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('` + upper + `', '` + upper + `')`)
	require.NoError(t, err)
	cleanReplication(t, slot, pub)
	t.Cleanup(func() { cleanReplication(t, slot, pub) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, 0, pr, po) }()

	payload := func(e *cluster.Entity) map[string]any {
		var m map[string]any
		require.NoError(t, json.Unmarshal(e.Data, &m))
		return m
	}
	snap := ingesttest.Await(t, pr, po, 30*time.Second, nil, lower)
	var seed *cluster.Entity
	for _, p := range snap.Proposals {
		for _, e := range p.Entities {
			if string(e.Key) == lower {
				seed = e
			}
		}
	}
	require.NotNil(t, seed, "the snapshot keys the row by the lowercase uuid")
	require.Equal(t, lower, payload(seed)["ref"], "snapshot payload: lowercase")

	waitForSlot(t, slot)
	const newUpper = "A1B2C3D4-0000-1111-2222-333344445555"
	const newLower = "a1b2c3d4-0000-1111-2222-333344445555"
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('` + newUpper + `', '` + upper + `')`)
	require.NoError(t, err)
	live := awaitCommit(t, pr, po, 30*time.Second, newLower)
	var row *cluster.Entity
	for _, p := range live.Proposals {
		for _, e := range p.Entities {
			if string(e.Key) == newLower {
				row = e
			}
		}
	}
	require.NotNil(t, row, "the stream keys the row by the lowercase uuid")
	require.Equal(t, lower, payload(row)["ref"], "stream payload: lowercase — byte-identical to the snapshot spelling")
}
