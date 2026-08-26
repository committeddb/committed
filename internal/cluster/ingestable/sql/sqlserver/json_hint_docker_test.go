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

// typerStub resolves any topic ref to a bare Type (the parser needs one).
type typerStub struct{}

func (typerStub) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	return &cluster.Type{ID: ref.ID, Name: ref.ID}, nil
}

// The pilot's rig-shaped reproduction attempt: hints resolved by the REAL
// parser (TOML jsonColumns + mapAllColumns — not hand-built mappings) and
// the CT change consumed by a RESUMED worker (restart from a streaming
// checkpoint — the pilot's re-registered ingest resumed, it did not run
// fresh). The field claim: stream-captured events store the hinted column
// as a STRING permanently. If this test is green, the asymmetry needs an
// ingredient the rig has and this shape lacks.
func TestSQLServerJSONColumnHintParserResume(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.json_hint_parser`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.json_hint_parser (pk INT NOT NULL PRIMARY KEY, EventData NVARCHAR(MAX))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.json_hint_parser (pk, EventData) VALUES (1, '{"z":1,"elements":[{"Id":"e1","amount":2.50}]}')`)
	require.NoError(t, err)

	toml := fmt.Sprintf(`
[ingestable]
name = "json-hint-parser"
type = "sql"

[sql]
dialect          = "sqlserver"
connectionString = %q
topic            = "json-hint-parser"
tables           = ["json_hint_parser"]
primaryKey       = "pk"
mapAllColumns    = true
jsonColumns      = ["EventData"]
poll_interval    = "300ms"
`, ingestURL)
	v, err := cluster.ParseConfigBytes("toml", []byte(toml))
	require.NoError(t, err)
	p := sql.NewIngestableParser(typerStub{})
	p.Dialects["sqlserver"] = &sqlserver.SQLServerDialect{}
	ing, err := p.Parse(v)
	require.NoError(t, err, "the parser path must admit (hints resolve onto map-all mappings)")

	// Run 1 (fresh): snapshot decodes the hinted column.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() { _ = ing.Ingest(ctx1, nil, pr1, po1) }()
	run1, checkpoint := drainUntilPosition(t, pr1, po1, 1, 2*time.Minute)
	require.Len(t, run1, 1)
	cancel1()
	var snapPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(run1[0].Data, &snapPayload))
	require.True(t, len(snapPayload["EventData"]) > 0 && snapPayload["EventData"][0] == '{',
		"snapshot: hinted column decodes to an object; got %s", snapPayload["EventData"])

	// Downtime change, then RESUME from the streaming checkpoint — the CT
	// path of a restarted worker, the rig's actual shape.
	_, err = db.Exec(`INSERT INTO dbo.json_hint_parser (pk, EventData) VALUES (2, '{"z":1,"elements":[{"Id":"e1","amount":2.50}]}')`)
	require.NoError(t, err)

	ing2, err := p.Parse(v)
	require.NoError(t, err)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() { _ = ing2.Ingest(ctx2, checkpoint, pr2, po2) }()
	live := drainEntities(t, pr2, po2, 1, 2*time.Minute)

	var ctPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(live[0].Data, &ctPayload))
	require.True(t, len(ctPayload["EventData"]) > 0 && ctPayload["EventData"][0] == '{',
		"CT-streamed on a RESUMED worker: hinted column must decode to an object, not a string; got %s", ctPayload["EventData"])
	require.Equal(t, string(snapPayload["EventData"]), string(ctPayload["EventData"]),
		"snapshot and resumed-CT renderings must be byte-identical")
}

// The two spellings the flat-form tests never exercised: (a) the
// [[sql.topics]] form with jsonColumns declared per entry — must hint;
// (b) the MIXED spelling — [[sql.topics]] entries with jsonColumns left
// at the flat position — which must be either honored or loudly
// rejected, never silently ignored (the admission-validation class: a
// silently dropped hint is exactly the field's string-payload shape).
func TestSQLServerJSONColumnHintTopicsFormSpellings(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.json_hint_topics`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.json_hint_topics (pk INT NOT NULL PRIMARY KEY, EventData NVARCHAR(MAX))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.json_hint_topics (pk, EventData) VALUES (1, '{"z":1,"a":2.50}')`)
	require.NoError(t, err)

	run := func(t *testing.T, toml string) map[string]json.RawMessage {
		t.Helper()
		v, err := cluster.ParseConfigBytes("toml", []byte(toml))
		require.NoError(t, err)
		p := sql.NewIngestableParser(typerStub{})
		p.Dialects["sqlserver"] = &sqlserver.SQLServerDialect{}
		ing, err := p.Parse(v)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pr := make(chan *cluster.Proposal, 64)
		po := make(chan cluster.Position, 64)
		go func() { _ = ing.Ingest(ctx, nil, pr, po) }()
		got := drainEntities(t, pr, po, 1, 2*time.Minute)
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(got[0].Data, &payload))
		return payload
	}

	t.Run("topics form, per-entry jsonColumns", func(t *testing.T) {
		payload := run(t, fmt.Sprintf(`
[ingestable]
name = "jh-topics"
type = "sql"

[sql]
dialect          = "sqlserver"
connectionString = %q

[[sql.topics]]
topic         = "jh-topics"
tables        = ["json_hint_topics"]
primaryKey    = "pk"
mapAllColumns = true
jsonColumns   = ["EventData"]
`, ingestURL))
		require.True(t, len(payload["EventData"]) > 0 && payload["EventData"][0] == '{',
			"per-entry jsonColumns in the topics form must hint; got %s", payload["EventData"])
	})

	t.Run("MIXED spelling: topics form, flat-position jsonColumns", func(t *testing.T) {
		toml := fmt.Sprintf(`
[ingestable]
name = "jh-mixed"
type = "sql"

[sql]
dialect          = "sqlserver"
connectionString = %q
jsonColumns      = ["EventData"]

[[sql.topics]]
topic         = "jh-mixed"
tables        = ["json_hint_topics"]
primaryKey    = "pk"
mapAllColumns = true
`, ingestURL)
		v, err := cluster.ParseConfigBytes("toml", []byte(toml))
		require.NoError(t, err)
		p := sql.NewIngestableParser(typerStub{})
		p.Dialects["sqlserver"] = &sqlserver.SQLServerDialect{}
		ing, err := p.Parse(v)
		if err != nil {
			// Loud rejection is an acceptable contract.
			require.Contains(t, err.Error(), "jsonColumns")
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pr := make(chan *cluster.Proposal, 64)
		po := make(chan cluster.Position, 64)
		go func() { _ = ing.Ingest(ctx, nil, pr, po) }()
		got := drainEntities(t, pr, po, 1, 2*time.Minute)
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(got[0].Data, &payload))
		require.True(t, len(payload["EventData"]) > 0 && payload["EventData"][0] == '{',
			"a flat-position jsonColumns with the topics form was ACCEPTED, so it must hint — silently ignoring it is the string-payload trap; got %s", payload["EventData"])
	})
}
