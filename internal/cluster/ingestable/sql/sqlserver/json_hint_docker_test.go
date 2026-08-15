//go:build docker

package sqlserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// The pilot's TransactionEvents shape: an NVARCHAR column holding JSON —
// undetectable as JSON from type metadata, so it used to arrive as one
// escaped string that no projection jsonPath could traverse. With the
// jsonColumns hint on the mapping, the column decodes as a REAL,
// canonicalized JSON value on BOTH ingest paths (snapshot row and CT
// change, the byte-parity contract), and a row whose hinted column holds
// invalid JSON falls back to the string — never an invalid payload.
func TestSQLServerJSONColumnHint(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.json_hint`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.json_hint (pk INT NOT NULL PRIMARY KEY, event_data NVARCHAR(MAX))`)
	require.NoError(t, err)
	// Snapshot row: valid JSON with deliberately unsorted keys and an exact
	// decimal — canonicalization must sort and preserve digits.
	_, err = db.Exec(`INSERT INTO dbo.json_hint (pk, event_data) VALUES (1, '{"z":1,"a":{"amount":2.50}}')`)
	require.NoError(t, err)

	typ := &cluster.Type{ID: "json-hint", Name: "json-hint"}
	config := &sql.Config{
		Type: typ,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "eventData", SQLColumn: "event_data", JSONHint: true},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"json_hint"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 256)
	po := make(chan cluster.Position, 256)
	d := &sqlserver.SQLServerDialect{}
	go func() { _ = d.Ingest(ctx, config, nil, 0, pr, po) }()

	snap := drainEntities(t, pr, po, 1, 2*time.Minute)
	require.JSONEq(t, `{"pk":1,"eventData":{"a":{"amount":2.50},"z":1}}`, string(snap[0].Data),
		"the hinted column must arrive as a real nested object, canonicalized")
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(snap[0].Data, &decoded))
	_, isObject := decoded["eventData"].(map[string]any)
	require.True(t, isObject, "eventData must be a traversable object, not an escaped string")

	// Live CT changes: a valid-JSON row (must match the snapshot rendering
	// byte-for-byte — one decode path) and an invalid-JSON row (string
	// fallback).
	_, err = db.Exec(`INSERT INTO dbo.json_hint (pk, event_data) VALUES (2, '{"z":1,"a":{"amount":2.50}}')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.json_hint (pk, event_data) VALUES (3, '{not json')`)
	require.NoError(t, err)

	live := drainEntities(t, pr, po, 2, 2*time.Minute)
	byKey := map[string]*cluster.Entity{}
	for _, e := range live {
		byKey[string(e.Key)] = e
	}

	var snapPayload, cdcPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap[0].Data, &snapPayload))
	require.NoError(t, json.Unmarshal(byKey["2"].Data, &cdcPayload))
	require.Equal(t, string(snapPayload["eventData"]), string(cdcPayload["eventData"]),
		"snapshot and CT must render the hinted column byte-identically (the parity contract)")

	require.JSONEq(t, fmt.Sprintf(`{"pk":3,"eventData":%q}`, "{not json"), string(byKey["3"].Data),
		"invalid JSON in a hinted column falls back to the string — never an invalid payload")
}
