//go:build docker || integration

package postgres_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// rowCount for chunking tests; sized to comfortably exceed batch_size=10.
const chunkTestRowCount = 25

var connString string

const (
	dbName   = "testdb"
	username = "postgres"
	password = "secret"
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16",
		tcpostgres.WithDatabase(dbName),
		tcpostgres.WithUsername(username),
		tcpostgres.WithPassword(password),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_replication_slots=16",
					"-c", "max_wal_senders=16",
				},
			},
		}),
	)
	if err != nil {
		log.Fatalf("Could not start Postgres container: %v", err)
	}

	connString, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("Could not get connection string: %v", err)
	}

	code := m.Run()

	// Ryuk (the testcontainers reaper) handles cleanup automatically.
	os.Exit(code)
}

func createDB(t *testing.T) *gosql.DB {
	db, err := gosql.Open("pgx", connString)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

// isCommitPosition reports whether a position published by the
// dialect's `po` channel is a per-commit checkpoint (Lsn>0) rather
// than a snapshot-progress checkpoint (Lsn=0, with SnapshotProgress
// set). encodePosition / decodePosition in postgres.go own the wire
// format: magic byte 0xff followed by a proto-encoded
// dialectpb.PostgresPosition.
func isCommitPosition(t *testing.T, pos cluster.Position) bool {
	t.Helper()
	return positionLSN(pos) > 0
}

// positionLSN returns the LSN of a per-commit checkpoint position as a
// WAL byte offset (comparable to pg_current_wal_lsn() - '0/0'), or 0
// for snapshot-progress checkpoints and undecodable positions.
func positionLSN(pos cluster.Position) uint64 {
	if len(pos) < 2 || pos[0] != 0xff {
		return 0
	}
	pp := &dialectpb.PostgresPosition{}
	if err := proto.Unmarshal(pos[1:], pp); err != nil {
		return 0
	}
	return pp.Lsn
}

// cleanReplication drops the named replication slot and publication if
// they survive from an earlier run against the same server (-count>1,
// or a crashed run). Leftovers don't fail loudly — they stream silence:
// a publication references tables by OID, so a prior run's DROP TABLE
// leaves the publication empty and the recreated table unpublished,
// while a leftover slot suppresses the initial snapshot. The slot drop
// retries because the previous run's walsender dies asynchronously
// after ctx cancel and an active slot cannot be dropped.
func cleanReplication(t *testing.T, slotName, pubName string) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	require.Eventually(t, func() bool {
		_, err := db.Exec(
			`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`,
			slotName)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)
	_, err := db.Exec(fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s`, pubName))
	require.NoError(t, err)
}

// waitForSlot polls pg_stat_replication until the named slot is in
// state='streaming' — i.e. the dialect has finished its snapshot phase
// and is actively consuming WAL. Gating on pg_replication_slots.active
// is NOT enough: active flips true when the dialect's replication
// connection opens (before snapshot), so a subsequent INSERT can race
// the dialect's snapshot REPEATABLE READ window, get captured BOTH by
// the snapshot AND re-streamed by pgoutput → two proposals from one
// INSERT, which broke TestPostgresTransactionGrouping flakily ~25% of
// the time.
func waitForSlot(t *testing.T, slotName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		db := createDB(t)
		defer db.Close()
		var state string
		err := db.QueryRow(`
			SELECT s.state
			FROM pg_stat_replication s
			JOIN pg_replication_slots r ON r.active_pid = s.pid
			WHERE r.slot_name = $1`, slotName,
		).Scan(&state)
		return err == nil && state == "streaming"
	}, 10*time.Second, 100*time.Millisecond)
}

func TestPostgresDialect(t *testing.T) {
	simpleType := &cluster.Type{
		ID:   "simple",
		Name: "simple",
	}
	basicConfig := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{{
			JsonName:  "one",
			SQLColumn: "one",
		}, {
			JsonName:  "pk",
			SQLColumn: "pk",
		}},
		PrimaryKey: []string{"pk"},
	}

	e1 := &cluster.Entity{
		Type: simpleType,
		Key:  []byte("key1"),
		Data: []byte(`{"one":"one","pk":"key1"}`),
	}
	e2 := &cluster.Entity{
		Type: simpleType,
		Key:  []byte("key2"),
		Data: []byte(`{"one":"two","pk":"key2"}`),
	}

	tests := []struct {
		name     string
		config   *sql.Config
		table    string
		insertFn func(*testing.T, string)
		entities []*cluster.Entity
	}{
		{"one_simple", basicConfig, "pgtest_one", insertOne, []*cluster.Entity{e1}},
		{"two_simple", basicConfig, "pgtest_two", insertTwo, []*cluster.Entity{e1, e2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the table before starting the dialect. The
			// publication references the table so it must exist.
			db := createDB(t)
			_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tt.table))
			require.NoError(t, err)
			_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, one TEXT)`, tt.table))
			require.NoError(t, err)
			db.Close()

			slotName := fmt.Sprintf("slot_%s", tt.name)
			pubName := fmt.Sprintf("pub_%s", tt.name)
			cleanReplication(t, slotName, pubName)

			dialect := &postgres.PostgreSQLDialect{}
			tt.config.ConnectionString = connString
			tt.config.Tables = []string{tt.table}
			tt.config.Options = map[string]string{
				"slot_name":   slotName,
				"publication": pubName,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			proposalChan := make(chan *cluster.Proposal, 10)
			positionChan := make(chan cluster.Position, 10)

			ingestErr := make(chan error, 1)
			go func() {
				ingestErr <- dialect.Ingest(ctx, tt.config, nil, proposalChan, positionChan)
			}()

			waitForSlot(t, slotName)

			// Insert rows AFTER the dialect is running — pgoutput
			// only captures changes from the slot creation point.
			tt.insertFn(t, tt.table)

			// Collect entities until we've seen the expected count.
			expected := len(tt.entities)
			deadline := time.After(15 * time.Second)
			seen := make(map[string]*cluster.Entity)
			var seqs []uint64
			for len(seen) < expected {
				select {
				case proposal := <-proposalChan:
					seqs = append(seqs, proposal.SourceSeq)
					for _, e := range proposal.Entities {
						seen[string(e.Key)] = e
					}
				case <-positionChan:
				case <-deadline:
					t.Fatalf("timed out waiting for %d unique entities; got %d", expected, len(seen))
				}
			}

			// Streaming-phase proposals must each carry a non-zero,
			// strictly-increasing SourceSeq (the commit/message LSN) — the
			// effectively-once dedup key the ingest worker relies on.
			for i, sq := range seqs {
				require.NotZero(t, sq, "CDC proposal %d must carry a non-zero SourceSeq (LSN)", i)
				if i > 0 {
					require.Greater(t, sq, seqs[i-1], "SourceSeq must strictly increase per proposal")
				}
			}

			entities := make([]*cluster.Entity, 0, len(seen))
			for _, e := range seen {
				entities = append(entities, e)
			}

			require.ElementsMatch(t, tt.entities, entities)

			cancel()
			select {
			case err := <-ingestErr:
				require.Nil(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Ingest did not exit after cancel")
			}
		})
	}
}

// TestPostgresPKChangingUpdateTombstonesOldKey is the orphan-row regression: an
// UPDATE that changes the primary key must emit a delete tombstone for the old
// key alongside the new-key upsert, so one source row maps to exactly one
// downstream row. Without the tombstone the old key lingers downstream forever
// (divergence + an un-deletable stale record — an RTBF concern). REPLICA
// IDENTITY DEFAULT populates the old tuple on a key change, so no FULL is needed.
func TestPostgresPKChangingUpdateTombstonesOldKey(t *testing.T) {
	const table = "pgtest_pkchange"
	simpleType := &cluster.Type{ID: "simple", Name: "simple"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "one", SQLColumn: "one"},
			{JsonName: "pk", SQLColumn: "pk"},
		},
		PrimaryKey: []string{"pk"},
	}

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, one TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	slotName, pubName := "slot_pkchange", "pub_pkchange"
	cleanReplication(t, slotName, pubName)

	dialect := &postgres.PostgreSQLDialect{}
	config.ConnectionString = connString
	config.Tables = []string{table}
	config.Options = map[string]string{"slot_name": slotName, "publication": pubName}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- dialect.Ingest(ctx, config, nil, proposalChan, positionChan) }()

	waitForSlot(t, slotName)

	// Insert then change the primary key. Both happen after the slot is ready so
	// pgoutput captures them: INSERT pk=old, then UPDATE pk old→new.
	db2 := createDB(t)
	_, err = db2.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('old', 'v')`, table))
	require.NoError(t, err)
	_, err = db2.Exec(fmt.Sprintf(`UPDATE %s SET pk = 'new' WHERE pk = 'old'`, table))
	require.NoError(t, err)
	db2.Close()

	// The PK-changing UPDATE must yield both a new-key upsert and an old-key
	// tombstone; timing out on either is the regression (fails today).
	haveNewUpsert, haveOldTombstone := false, false
	deadline := time.After(15 * time.Second)
	for !haveNewUpsert || !haveOldTombstone {
		select {
		case proposal := <-proposalChan:
			for _, e := range proposal.Entities {
				switch {
				case string(e.Key) == "new" && !e.IsDelete():
					haveNewUpsert = true
				case string(e.Key) == "new" && e.IsDelete():
					t.Fatal("the new key must be an upsert, not a tombstone")
				case string(e.Key) == "old" && e.IsDelete():
					haveOldTombstone = true
				}
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out: new-key upsert=%v old-key tombstone=%v", haveNewUpsert, haveOldTombstone)
		}
	}

	cancel()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresTruncateNotSilentlyDropped is the truncate-divergence guard: a
// source TRUNCATE on a watched table is not propagated (committed has no
// clear-all primitive), but it must NOT be swallowed silently — it must be logged
// at Warn naming the affected table so an operator can alert and re-snapshot.
func TestPostgresTruncateNotSilentlyDropped(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	const table = "pgtest_truncate"
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_trunc", "pub_trunc")

	config := &sql.Config{
		Type:             &cluster.Type{ID: "trunc", Name: "trunc"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options:          map[string]string{"slot_name": "slot_trunc", "publication": "pub_trunc"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	dialect := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- dialect.Ingest(ctx, config, nil, proposalChan, positionChan) }()

	waitForSlot(t, "slot_trunc")

	// Insert then TRUNCATE the watched table (both after the slot is ready).
	db2 := createDB(t)
	_, err = db2.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('a','1')`, table))
	require.NoError(t, err)
	_, err = db2.Exec(fmt.Sprintf(`TRUNCATE %s`, table))
	require.NoError(t, err)
	db2.Close()

	// Drain the stream while watching for the loud, table-named TRUNCATE warning.
	deadline := time.After(20 * time.Second)
	for {
		got := observed.FilterMessageSnippet("TRUNCATE on a watched table is not propagated").
			FilterField(zap.Strings("tables", []string{"public." + table})).All()
		if len(got) > 0 {
			break
		}
		select {
		case <-proposalChan:
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out: the TRUNCATE must be logged loudly, naming the table")
		}
	}

	cancel()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresTypedPayload is the type-fidelity criterion on real Postgres:
// int/numeric/bool/jsonb columns ingest as their natural JSON types (not
// strings), and a row captured by the snapshot has the same JSON shape as a row
// captured by CDC. One row is inserted before Ingest starts (snapshot) and one
// after the slot is ready (CDC); both must come back typed and identical in
// shape.
func TestPostgresTypedPayload(t *testing.T) {
	const table = "pgtest_typed"
	typedType := &cluster.Type{ID: "typed", Name: "typed"}
	config := &sql.Config{
		Type:             typedType,
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"pk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "n", SQLColumn: "n"},
			{JsonName: "r", SQLColumn: "r"},
			{JsonName: "b", SQLColumn: "b"},
			{JsonName: "j", SQLColumn: "j"},
		},
		Options: map[string]string{"slot_name": "slot_typed", "publication": "pub_typed"},
	}

	db := createDB(t)
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (pk VARCHAR(32) PRIMARY KEY, n INT, r NUMERIC, b BOOL, j JSONB)`)
	require.NoError(t, err)
	// Snapshot row — inserted before the dialect starts.
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('a', 1994, 9.50, true, '{"k":1}')`)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_typed", "pub_typed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_typed")

	// CDC row — inserted after the slot exists.
	db = createDB(t)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('b', 2008, 9.00, false, '{"k":2}')`)
	require.NoError(t, err)
	db.Close()

	seen := map[string]*cluster.Entity{}
	deadline := time.After(20 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out; got %d of 2 entities", len(seen))
		}
	}
	cancel()

	// decode with UseNumber so a JSON number stays a json.Number (not float64),
	// letting us assert the column came through as a number, not a string.
	typed := func(key string) map[string]any {
		dec := json.NewDecoder(bytes.NewReader(seen[key].Data))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		return m
	}
	for _, key := range []string{"a", "b"} {
		m := typed(key)
		require.IsType(t, json.Number(""), m["n"], "int column is a JSON number")
		require.IsType(t, json.Number(""), m["r"], "numeric column is a JSON number")
		require.IsType(t, false, m["b"], "bool column is a JSON bool")
		require.IsType(t, map[string]any{}, m["j"], "jsonb column embeds as an object, not a string")
		require.IsType(t, "", m["pk"], "text column stays a string")
	}
	require.Equal(t, "1994", typed("a")["n"].(json.Number).String(), "snapshot int value")
	require.Equal(t, "2008", typed("b")["n"].(json.Number).String(), "CDC int value")
	require.Equal(t, true, typed("a")["b"], "snapshot bool")
	require.Equal(t, false, typed("b")["b"], "CDC bool")

	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresSnapshotStreamByteIdentity is the snapshot==CDC invariant for the
// types that used to diverge: a timestamp/date/timestamptz/bytea/numeric row read
// by the snapshot must produce byte-identical payload bytes to the same row read
// by CDC. Before the ::text-cast fix, the snapshot went through pgx typed decode
// (time.Time → RFC3339, bytea → raw/base64, numeric → float64) while CDC used
// pgoutput text, so the same value differed and flipped format on its first CDC
// update after snapshot.
func TestPostgresSnapshotStreamByteIdentity(t *testing.T) {
	const table = "pgtest_byteident"
	config := &sql.Config{
		Type:             &cluster.Type{ID: "byteident", Name: "byteident"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"pk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "ts", SQLColumn: "ts"},
			{JsonName: "tstz", SQLColumn: "tstz"},
			{JsonName: "d", SQLColumn: "d"},
			{JsonName: "by", SQLColumn: "by"},
			{JsonName: "num", SQLColumn: "num"},
		},
		Options: map[string]string{"slot_name": "slot_byteident", "publication": "pub_byteident"},
	}

	db := createDB(t)
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table +
		` (pk VARCHAR(32) PRIMARY KEY, ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, by BYTEA, num NUMERIC)`)
	require.NoError(t, err)
	// The snapshot row and the CDC row carry IDENTICAL values (only the pk
	// differs), so any byte difference between their payloads is the bug.
	const vals = `'2024-01-15 10:30:00', '2024-01-15 10:30:00+00', '2024-01-15', '\x48656c6c6f', 9.50`
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('a', ` + vals + `)`)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_byteident", "pub_byteident")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_byteident")

	db = createDB(t)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('b', ` + vals + `)`)
	require.NoError(t, err)
	db.Close()

	seen := map[string][]byte{}
	deadline := time.After(20 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e.Data
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out; got %d of 2 entities", len(seen))
		}
	}
	cancel()

	// UseNumber so a numeric stays json.Number (exact source text), not float64 —
	// otherwise "9.50" would compare equal to "9.5" and hide a trailing-zero drift.
	field := func(payload []byte, name string) any {
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		return m[name]
	}
	// The invariant: snapshot ('a') and CDC ('b') emit identical bytes per field.
	for _, col := range []string{"ts", "tstz", "d", "by", "num"} {
		require.Equal(t, field(seen["a"], col), field(seen["b"], col),
			"snapshot and CDC must emit identical %q bytes for the same value", col)
	}
	// And the form is the Postgres text form, not pgx's typed rendering: a
	// timestamp has no 'T' (RFC3339 would, if a time.Time leaked through), bytea is
	// hex-prefixed (not base64/raw), and numeric keeps its trailing zero.
	require.NotContains(t, field(seen["a"], "ts"), "T", "timestamp is Postgres text, not RFC3339")
	require.Contains(t, field(seen["a"], "by"), `\x`, "bytea is hex text, not base64/raw")
	require.Equal(t, json.Number("9.50"), field(seen["a"], "num"), "numeric keeps its exact source text")

	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresSnapshotStreamDomainByteIdentity is the snapshot==CDC invariant for
// user-defined DOMAIN columns. The snapshot classifies a domain by its base type
// (DatabaseTypeName reports "NUMERIC"/"BOOL"), so it renders a domain-over-numeric
// as a JSON number; CDC gets the domain's own OID in the relation message, which
// is unknown to pgCategoryForOID and so fell through to a string — diverging on
// the first CDC update after snapshot. The fix resolves the domain OID to its
// base type on the CDC path.
func TestPostgresSnapshotStreamDomainByteIdentity(t *testing.T) {
	const table = "pgtest_domainident"
	config := &sql.Config{
		Type:             &cluster.Type{ID: "domainident", Name: "domainident"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"pk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "amt", SQLColumn: "amt"},
			{JsonName: "flag", SQLColumn: "flag"},
		},
		Options: map[string]string{"slot_name": "slot_domainident", "publication": "pub_domainident"},
	}

	db := createDB(t)
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	// Drop the domains after the table (the table depends on them), then recreate.
	_, err = db.Exec(`DROP DOMAIN IF EXISTS test_amount`)
	require.NoError(t, err)
	_, err = db.Exec(`DROP DOMAIN IF EXISTS test_flag`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE DOMAIN test_amount AS NUMERIC(12,2)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE DOMAIN test_flag AS BOOLEAN`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table +
		` (pk VARCHAR(32) PRIMARY KEY, amt test_amount, flag test_flag)`)
	require.NoError(t, err)
	const vals = `9.50, true`
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('a', ` + vals + `)`)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_domainident", "pub_domainident")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_domainident")

	db = createDB(t)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('b', ` + vals + `)`)
	require.NoError(t, err)
	db.Close()

	seen := map[string][]byte{}
	deadline := time.After(20 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e.Data
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out; got %d of 2 entities", len(seen))
		}
	}
	cancel()

	field := func(payload []byte, name string) any {
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		return m[name]
	}
	for _, col := range []string{"amt", "flag"} {
		require.Equal(t, field(seen["a"], col), field(seen["b"], col),
			"snapshot and CDC must emit identical %q bytes for the same value", col)
	}
	// The domain resolves to its base type on both paths: numeric → number
	// (exact text), boolean → bool — not a quoted string.
	require.Equal(t, json.Number("9.50"), field(seen["a"], "amt"),
		"domain-over-numeric renders as an exact number")
	require.Equal(t, true, field(seen["a"], "flag"),
		"domain-over-bool renders as a bool")

	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresTeardownSourceDropsSlot is the slot-leak success criterion:
// tearing down a deleted ingestable drops its replication slot (and publication)
// on the source, so the orphaned slot can't pin the source's WAL and fill its
// disk. Idempotent — a second teardown after the slot is gone is a clean no-op.
func TestPostgresTeardownSourceDropsSlot(t *testing.T) {
	const table = "pgtest_teardown"
	db := createDB(t)
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (pk VARCHAR(32) PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_teardown", "pub_teardown")

	config := &sql.Config{
		Type:             &cluster.Type{ID: "teardown", Name: "teardown"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options:          map[string]string{"slot_name": "slot_teardown", "publication": "pub_teardown"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	dialect := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- dialect.Ingest(ctx, config, nil, proposalChan, positionChan) }()

	waitForSlot(t, "slot_teardown") // the slot now exists on the source

	// Stop the worker so the slot goes inactive — a live slot can't be dropped.
	cancel()
	select {
	case <-ingestErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}

	require.NoError(t, dialect.TeardownSource(config))

	check := createDB(t)
	defer check.Close()
	slotCount := func() int {
		var n int
		require.NoError(t, check.QueryRow(
			`SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1`, "slot_teardown").Scan(&n))
		return n
	}
	var pubs int
	require.NoError(t, check.QueryRow(
		`SELECT COUNT(*) FROM pg_publication WHERE pubname = $1`, "pub_teardown").Scan(&pubs))
	require.Equal(t, 0, slotCount(), "the replication slot must be dropped")
	require.Equal(t, 0, pubs, "the publication must be dropped")

	// Idempotent: a second teardown after the slot is already gone is a no-op.
	require.NoError(t, dialect.TeardownSource(config))
	require.Equal(t, 0, slotCount())
}

// TestPostgresMixedCaseColumn is the mixed-case-column regression on real
// Postgres: a quoted CamelCase source column ("CreatedAt") mapped by a
// case-matching config must carry its value through BOTH the snapshot and CDC
// paths, not silently emit null. Before the fix the lookup used the config's
// exact case against the lowercased decode map and missed.
func TestPostgresMixedCaseColumn(t *testing.T) {
	const table = "pgtest_mixedcase"
	mcType := &cluster.Type{ID: "mixedcase", Name: "mixedcase"}
	config := &sql.Config{
		Type:             mcType,
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"pk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "createdAt", SQLColumn: "CreatedAt"}, // quoted CamelCase source column
		},
		Options: map[string]string{"slot_name": "slot_mixedcase", "publication": "pub_mixedcase"},
	}

	db := createDB(t)
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (pk VARCHAR(32) PRIMARY KEY, "CreatedAt" TEXT)`)
	require.NoError(t, err)
	// Snapshot row — inserted before the dialect starts.
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('a', 'snap-value')`)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_mixedcase", "pub_mixedcase")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_mixedcase")

	// CDC row — inserted after the slot exists.
	db = createDB(t)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('b', 'cdc-value')`)
	require.NoError(t, err)
	db.Close()

	seen := map[string]*cluster.Entity{}
	deadline := time.After(20 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out; got %d of 2 entities", len(seen))
		}
	}
	cancel()

	field := func(key string) any {
		var m map[string]any
		require.NoError(t, json.Unmarshal(seen[key].Data, &m))
		return m["createdAt"]
	}
	require.Equal(t, "snap-value", field("a"), "snapshot: mixed-case column must carry its value, not null")
	require.Equal(t, "cdc-value", field("b"), "CDC: mixed-case column must carry its value, not null")

	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresPreflightReplicaIdentity is the guard's success criterion on real
// Postgres: a table whose replica identity carries the configured key passes;
// one that would drop the key on delete fails loud — and it's coverage, not
// "require FULL" (DEFAULT + a matching PK passes).
func TestPostgresPreflightReplicaIdentity(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	mk := func(q string) { _, err := db.Exec(q); require.NoError(t, err) }
	mk(`DROP TABLE IF EXISTS pf_default, pf_full, pf_nothing, pf_nopk, pf_comp`)
	mk(`CREATE TABLE pf_default (id TEXT PRIMARY KEY, v TEXT)`) // DEFAULT + PK → covered
	mk(`CREATE TABLE pf_full (id TEXT PRIMARY KEY, v TEXT)`)
	mk(`ALTER TABLE pf_full REPLICA IDENTITY FULL`)
	mk(`CREATE TABLE pf_nothing (id TEXT PRIMARY KEY, v TEXT)`)
	mk(`ALTER TABLE pf_nothing REPLICA IDENTITY NOTHING`)
	mk(`CREATE TABLE pf_nopk (id TEXT, v TEXT)`) // no PK + DEFAULT → nothing survives
	mk(`CREATE TABLE pf_comp (a TEXT, b INT, v TEXT, PRIMARY KEY (a, b))`)

	dialect := &postgres.PostgreSQLDialect{}
	cfg := func(table string, pk ...string) *sql.Config {
		return &sql.Config{ConnectionString: connString, Tables: []string{table}, PrimaryKey: pk}
	}

	require.NoError(t, dialect.Preflight(cfg("pf_default", "id")), "DEFAULT + PK covers the key (no FULL needed)")
	require.NoError(t, dialect.Preflight(cfg("pf_full", "id")), "FULL covers everything")
	require.NoError(t, dialect.Preflight(cfg("pf_comp", "a", "b")), "DEFAULT + composite PK covers a composite key")
	require.Error(t, dialect.Preflight(cfg("pf_nothing", "id")), "NOTHING drops the key on delete")
	require.Error(t, dialect.Preflight(cfg("pf_nopk", "id")), "no PK + DEFAULT drops the key on delete")

	err := dialect.Preflight(cfg("pf_nothing", "id"))
	require.Contains(t, err.Error(), "silently drop deletes")
	require.Contains(t, err.Error(), "REPLICA IDENTITY FULL", "the error is actionable")
}

func insertOne(t *testing.T, table string) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('key1', 'one')`, table))
	require.NoError(t, err)
}

func insertTwo(t *testing.T, table string) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('key1', 'one')`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, one) VALUES ('key2', 'two')`, table))
	require.NoError(t, err)
}

// TestPostgresUnchangedToastReselect is the TOAST regression end-to-end against
// real Postgres: a row with a large out-of-line (TOASTed) column, updated on a
// DIFFERENT column, must keep the TOASTed value downstream. Postgres omits the
// unchanged TOASTed column from the pgoutput UPDATE new tuple ('u'); the ingest
// re-selects it so the row stays complete instead of nulling the column.
func TestPostgresUnchangedToastReselect(t *testing.T) {
	const table = "pgtest_toast"
	big := string(bytes.Repeat([]byte("abcdefgh"), 1024)) // 8 KB, > the ~2 KB TOAST threshold

	config := &sql.Config{
		Type:             &cluster.Type{ID: "toast", Name: "toast"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"pk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "n", SQLColumn: "n"},
			{JsonName: "big", SQLColumn: "big"},
		},
		Options: map[string]string{"slot_name": "slot_toast", "publication": "pub_toast"},
	}

	db := createDB(t)
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (pk VARCHAR(32) PRIMARY KEY, n INT, big TEXT)`)
	require.NoError(t, err)
	// EXTERNAL storage disables compression, so the value is stored out-of-line
	// (TOASTed) and an UPDATE of another column reports it as unchanged ('u').
	_, err = db.Exec(`ALTER TABLE ` + table + ` ALTER COLUMN big SET STORAGE EXTERNAL`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO `+table+` VALUES ('a', 1, $1)`, big)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_toast", "pub_toast")
	// Drop this run's slot at the end so it doesn't hold a replication slot for
	// the rest of the suite (the container caps max_replication_slots).
	defer cleanReplication(t, "slot_toast", "pub_toast")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_toast")

	// Update a DIFFERENT column; Postgres leaves the TOASTed `big` out of the
	// UPDATE's new tuple.
	db = createDB(t)
	_, err = db.Exec(`UPDATE ` + table + ` SET n = 2 WHERE pk = 'a'`)
	require.NoError(t, err)
	db.Close()

	// Collect until the UPDATE (n == 2) arrives, then assert big survived.
	var updated map[string]any
	deadline := time.After(20 * time.Second)
	for updated == nil {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if string(e.Key) != "a" {
					continue
				}
				var m map[string]any
				require.NoError(t, json.Unmarshal(e.Data, &m))
				if m["n"] == float64(2) {
					updated = m
				}
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for the UPDATE entity (n=2)")
		}
	}
	cancel()

	require.Equal(t, big, updated["big"],
		"an unchanged TOASTed column must be re-selected and preserved, not nulled by the partial UPDATE")
}

// TestPostgresPositionResume verifies that checkpointed LSN positions are
// correctly restored on restart: a new Ingest call with a previously
// checkpointed position only receives changes committed after that LSN.
func TestPostgresPositionResume(t *testing.T) {
	table := "resume_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_resume", "pub_resume")

	simpleType := &cluster.Type{ID: "resume", Name: "resume"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_resume",
			"publication": "pub_resume",
		},
	}

	// --- Phase 1: start dialect, insert "before", collect position ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	proposalChan1 := make(chan *cluster.Proposal, 10)
	positionChan1 := make(chan cluster.Position, 10)

	dialect1 := &postgres.PostgreSQLDialect{}
	go func() {
		_ = dialect1.Ingest(ctx1, config, nil, proposalChan1, positionChan1)
	}()

	waitForSlot(t, "slot_resume")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('before', 'initial')`, table))
	require.NoError(t, err)
	db.Close()

	// Collect the "before" proposal AND a post-commit position.
	// The dialect emits TWO kinds of position checkpoints to po:
	//   - snapshot-progress (Lsn=0, SnapshotProgress set)
	//   - per-commit (Lsn>0, SnapshotProgress nil)
	// Resuming from a snapshot-progress checkpoint races re-streaming
	// from the slot's restart_lsn (the entire failure mode this test
	// is supposed to guard against). Filter explicitly: a usable
	// resume position is one whose decoded Lsn > 0.
	deadline := time.After(15 * time.Second)
	seen := make(map[string]bool)
	var lastPos cluster.Position
	for !seen["before"] || lastPos == nil {
		select {
		case p := <-proposalChan1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case pos := <-positionChan1:
			if isCommitPosition(t, pos) {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("timed out waiting for initial proposal and commit position")
		}
	}

	// Stop the first dialect.
	cancel1()
	require.NotEmpty(t, lastPos, "should have a checkpointed position")

	// --- Phase 2: start new dialect from checkpointed position, insert "after" ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	proposalChan2 := make(chan *cluster.Proposal, 10)
	positionChan2 := make(chan cluster.Position, 10)

	dialect2 := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect2.Ingest(ctx2, config, lastPos, proposalChan2, positionChan2)
	}()

	waitForSlot(t, "slot_resume")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('after', 'resumed')`, table))
	require.NoError(t, err)
	db.Close()

	// Collect: "after" should appear, "before" should NOT re-appear.
	deadline = time.After(15 * time.Second)
	seen2 := make(map[string]bool)
	for !seen2["after"] {
		select {
		case p := <-proposalChan2:
			for _, e := range p.Entities {
				seen2[string(e.Key)] = true
			}
		case <-positionChan2:
		case <-deadline:
			t.Fatal("timed out waiting for post-resume proposal")
		}
	}

	require.False(t, seen2["before"],
		"'before' should not re-appear when resuming from checkpointed position")

	cancel2()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresTransactionGrouping verifies that multiple rows committed in
// a single Postgres transaction arrive as exactly one cluster.Proposal with
// all entities.
func TestPostgresTransactionGrouping(t *testing.T) {
	table := "tx_group_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_txgroup", "pub_txgroup")

	simpleType := &cluster.Type{ID: "txgroup", Name: "txgroup"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_txgroup",
			"publication": "pub_txgroup",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &postgres.PostgreSQLDialect{}
	go func() {
		_ = dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	waitForSlot(t, "slot_txgroup")

	// Insert a sentinel row so we know streaming is active.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('sentinel', 'init')`, table))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(15 * time.Second)
	sentinelSeen := false
	for !sentinelSeen {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if string(e.Key) == "sentinel" {
					sentinelSeen = true
				}
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for sentinel row")
		}
	}

	// Insert 10 rows in a single transaction. Capture the WAL position
	// first: the commit's checkpoint must decode to an LSN past it,
	// which is what distinguishes the transaction's own checkpoint from
	// the sentinel commit's.
	db = createDB(t)
	var preCommitLSN int64
	require.NoError(t, db.QueryRow(`SELECT (pg_current_wal_lsn() - '0/0')::bigint`).Scan(&preCommitLSN))
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('tx%d', 'value%d')`, table, i, i))
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)
	db.Close()

	// All 10 rows must arrive as a single proposal, and the commit must
	// checkpoint a position past preCommitLSN. One loop waits for both:
	// select picks randomly among ready channels, so the earlier shape —
	// drain-and-discard positions while waiting for the proposal, then
	// wait for the position afterward — could discard the commit's
	// checkpoint (the last position the dialect ever emits here) in the
	// drain and time out on a channel nothing would ever send to again.
	deadline = time.After(15 * time.Second)
	var txProposal *cluster.Proposal
	postCommitSeen := false
	for txProposal == nil || !postCommitSeen {
		select {
		case p := <-proposalChan:
			txProposal = p
		case pos := <-positionChan:
			if positionLSN(pos) > uint64(preCommitLSN) {
				postCommitSeen = true
			}
		case <-deadline:
			if txProposal == nil {
				t.Fatal("timed out waiting for transaction proposal")
			}
			t.Fatal("timed out waiting for post-commit position")
		}
	}

	require.Len(t, txProposal.Entities, 10,
		"expected all 10 rows from one transaction in a single proposal")

	keys := make(map[string]bool)
	for _, e := range txProposal.Entities {
		keys[string(e.Key)] = true
	}
	for i := 0; i < 10; i++ {
		require.True(t, keys[fmt.Sprintf("tx%d", i)], "missing key tx%d", i)
	}
}

// TestPostgresSnapshotOnNewSlot verifies that pre-existing rows are
// delivered as proposals when a new replication slot is created
// (initial snapshot), and that streaming picks up new changes
// seamlessly afterward.
func TestPostgresSnapshotOnNewSlot(t *testing.T) {
	table := "snap_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)

	// Insert rows BEFORE starting the dialect — these must arrive via snapshot.
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('pre1', 'existing1')`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('pre2', 'existing2')`, table))
	require.NoError(t, err)
	db.Close()

	// Repeatability matters doubly here: this test's whole premise is a
	// NEW slot (snapshot on creation), so a leftover slot from a prior
	// run wouldn't just stream silence — it would skip the snapshot.
	cleanReplication(t, "slot_snap", "pub_snap")

	simpleType := &cluster.Type{ID: "snap", Name: "snap"}
	config := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_snap",
			"publication": "pub_snap",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	// Collect snapshot entities — the 2 pre-existing rows.
	deadline := time.After(15 * time.Second)
	seen := make(map[string]*cluster.Entity)
	for len(seen) < 2 {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot entities; got %d", len(seen))
		}
	}

	require.Contains(t, seen, "pre1")
	require.Contains(t, seen, "pre2")

	// Now wait for the slot to be active and insert a new row to verify
	// streaming works after the snapshot.
	waitForSlot(t, "slot_snap")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('post1', 'streamed')`, table))
	require.NoError(t, err)
	db.Close()

	deadline = time.After(15 * time.Second)
	for seen["post1"] == nil {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = e
			}
		case <-positionChan:
		case <-deadline:
			t.Fatal("timed out waiting for streamed entity after snapshot")
		}
	}

	require.NotNil(t, seen["post1"])

	cancel()
	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestPostgresSnapshotChunking verifies keyset-paginated snapshots
// deliver all rows across multiple proposals when batch_size is smaller
// than the row count.
func TestPostgresSnapshotChunking(t *testing.T) {
	// Capture logs to assert the snapshot never logs the primary-key value
	// (snapshot-primarykey-logged-info-pii): natural keys are often source PII and
	// the batch line is Info-level. Installed before Ingest so the snapshot's logs
	// are observed.
	core, observed := observer.New(zap.InfoLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	table := "pg_chunk_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)

	for i := 0; i < chunkTestRowCount; i++ {
		_, err = db.Exec(
			fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('%03d', 'v%d')`, table, i, i),
		)
		require.NoError(t, err)
	}
	db.Close()

	cleanReplication(t, "slot_chunk", "pub_chunk")

	config := &sql.Config{
		Type: &cluster.Type{ID: "chunk", Name: "chunk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_chunk",
			"publication": "pub_chunk",
			"batch_size":  "10",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 20)
	positionChan := make(chan cluster.Position, 20)

	dialect := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect.Ingest(ctx, config, nil, proposalChan, positionChan)
	}()

	deadline := time.After(20 * time.Second)
	seen := make(map[string]bool)
	snapshotProposals := 0
	for len(seen) < chunkTestRowCount {
		select {
		case p := <-proposalChan:
			snapshotProposals++
			require.LessOrEqual(t, len(p.Entities), 10,
				"each snapshot proposal must not exceed batch_size")
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out waiting for %d rows; got %d", chunkTestRowCount, len(seen))
		}
	}

	require.GreaterOrEqual(t, snapshotProposals, 3,
		"25 rows at batch_size=10 should produce ≥3 proposals")

	for i := 0; i < chunkTestRowCount; i++ {
		require.Truef(t, seen[fmt.Sprintf("%03d", i)], "missing row %03d", i)
	}

	// PII guard: the per-batch "snapshot: batch flushed" line must not carry the
	// primary key. The batch/row counts give progress; the PK stays out of logs.
	batchLogs := observed.FilterMessage("snapshot: batch flushed").All()
	require.NotEmpty(t, batchLogs, "the multi-batch snapshot must log at least one flushed batch")
	for _, e := range batchLogs {
		_, hasPK := e.ContextMap()["last_pk"]
		require.False(t, hasPK, "snapshot batch log must not include the primary-key value")
	}
}

// TestPostgresSnapshotCompositePrimaryKey is the regression for the silent
// row-loss the IMDb slice surfaced: a table with a composite PK (e.g. principals
// keyed by (tconst, ordering)) whose rows share a leading column. Keyed by one
// column they collided and all but the last were dropped. With the composite key
// every row gets a distinct entity key, and keyset pagination uses row-value
// comparison so a batch boundary inside a shared tconst doesn't skip its
// siblings — batch_size=2 forces exactly that boundary.
func TestPostgresSnapshotCompositePrimaryKey(t *testing.T) {
	table := "pg_composite_pk"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (tconst VARCHAR(16) NOT NULL, ordering INT NOT NULL, nconst TEXT, PRIMARY KEY (tconst, ordering))`,
		table))
	require.NoError(t, err)

	// Two movies; the first has three principals — the boundary case.
	rows := [][2]any{
		{"tt1", 1}, {"tt1", 2}, {"tt1", 3}, {"tt2", 1}, {"tt2", 2},
	}
	for i, r := range rows {
		_, err = db.Exec(
			fmt.Sprintf(`INSERT INTO %s (tconst, ordering, nconst) VALUES ('%s', %d, 'nm%d')`, table, r[0], r[1], i),
		)
		require.NoError(t, err)
	}
	db.Close()

	cleanReplication(t, "slot_composite", "pub_composite")

	config := &sql.Config{
		Type: &cluster.Type{ID: "principal", Name: "principal"},
		Mappings: []sql.Mapping{
			{JsonName: "tconst", SQLColumn: "tconst"},
			{JsonName: "ordering", SQLColumn: "ordering"},
			{JsonName: "nconst", SQLColumn: "nconst"},
		},
		PrimaryKey:       []string{"tconst", "ordering"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_composite",
			"publication": "pub_composite",
			"batch_size":  "2", // forces a batch boundary inside tt1's principals
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 20)
	positionChan := make(chan cluster.Position, 20)

	dialect := &postgres.PostgreSQLDialect{}
	go func() { _ = dialect.Ingest(ctx, config, nil, proposalChan, positionChan) }()

	want := map[string]bool{
		`["tt1","1"]`: true, `["tt1","2"]`: true, `["tt1","3"]`: true,
		`["tt2","1"]`: true, `["tt2","2"]`: true,
	}
	seen := make(map[string]bool)
	deadline := time.After(20 * time.Second)
	for len(seen) < len(want) {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out; want %d composite-keyed rows, got %d: %v", len(want), len(seen), seen)
		}
	}
	require.Equal(t, want, seen, "every composite-PK row must land with a distinct key (no collision)")
}

// dropSlotWhenInactive waits for the named replication slot to be released by a
// stopping ingest, then drops it — modelling an out-of-band slot loss (an
// operator dropping it, expiry, or a max_slot_wal_keep_size reap).
func dropSlotWhenInactive(t *testing.T, slotName string) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	require.Eventually(t, func() bool {
		var active bool
		if err := db.QueryRow(
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&active); err != nil {
			return false // not present yet
		}
		if active {
			return false // still held by the stopping ingest
		}
		_, derr := db.Exec(`SELECT pg_drop_replication_slot($1)`, slotName)
		return derr == nil
	}, 15*time.Second, 200*time.Millisecond, "slot should become inactive and droppable")
}

// TestPostgresSlotRecreatedResnapshots is the slot-ConsistentPoint regression: if
// the replication slot is dropped (operator, expiry, or a max_slot_wal_keep_size
// reap) while a prior checkpoint survives, resuming from that now-stale LSN would
// silently skip every change between it and the recreated slot's ConsistentPoint.
// The dialect must instead re-snapshot from the new ConsistentPoint. A 'gap' row
// is inserted after the slot is dropped but before the restart; the resumed
// ingest must capture it via the re-snapshot (without the fix it is lost).
func TestPostgresSlotRecreatedResnapshots(t *testing.T) {
	table := "slotrecreate_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_recreate", "pub_recreate")
	// Drop this run's slot at the end so it doesn't hold a replication slot for
	// the rest of the suite (the container caps max_replication_slots).
	defer cleanReplication(t, "slot_recreate", "pub_recreate")

	config := &sql.Config{
		Type:             &cluster.Type{ID: "slotrecreate", Name: "slotrecreate"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options:          map[string]string{"slot_name": "slot_recreate", "publication": "pub_recreate"},
	}

	// Phase 1: ingest, stream 'before', capture the commit position that will
	// become stale, then stop.
	ctx1, cancel1 := context.WithCancel(context.Background())
	proposalChan1 := make(chan *cluster.Proposal, 10)
	positionChan1 := make(chan cluster.Position, 10)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx1, config, nil, proposalChan1, positionChan1) }()
	waitForSlot(t, "slot_recreate")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('before', 'v1')`, table))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(15 * time.Second)
	seen := map[string]bool{}
	var lastPos cluster.Position
	for !seen["before"] || lastPos == nil {
		select {
		case p := <-proposalChan1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case pos := <-positionChan1:
			if isCommitPosition(t, pos) {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("phase 1: timed out waiting for 'before' + a commit position")
		}
	}
	cancel1()
	require.NotEmpty(t, lastPos, "should have a checkpointed commit position")

	// Drop ONLY the slot (keep the publication, so this is a slot-recreate, not a
	// table-add backfill), then insert 'gap' — the row a resume from the stale
	// LSN would skip.
	dropSlotWhenInactive(t, "slot_recreate")
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('gap', 'v2')`, table))
	require.NoError(t, err)
	db.Close()

	// Phase 2: resume from the (now stale) checkpoint. The slot is gone, so the
	// dialect must re-snapshot from the new ConsistentPoint and capture 'gap'.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	proposalChan2 := make(chan *cluster.Proposal, 10)
	positionChan2 := make(chan cluster.Position, 10)
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- (&postgres.PostgreSQLDialect{}).Ingest(ctx2, config, lastPos, proposalChan2, positionChan2)
	}()
	waitForSlot(t, "slot_recreate")

	deadline = time.After(20 * time.Second)
	genByKey := map[string]uint64{}
	var sawMarker bool
	var markerEpoch uint64
	for {
		if _, haveGap := genByKey["gap"]; haveGap && sawMarker {
			break
		}
		select {
		case p := <-proposalChan2:
			for _, e := range p.Entities {
				// The re-snapshot closes with a refresh-boundary marker at the
				// bumped epoch; a keyed sink sweeps rows left at an older one.
				if e.IsRefreshBoundary() {
					sawMarker = true
					markerEpoch = e.Generation
					continue
				}
				genByKey[string(e.Key)] = e.Generation
			}
		case <-positionChan2:
		case <-deadline:
			t.Fatal("phase 2: 'gap' + a refresh-boundary marker never arrived — a slot-recreate must re-snapshot and close with a reconciling marker")
		}
	}
	cancel2()

	// The gap-recovery re-snapshot bumped the epoch to 2 (phase 1 was the
	// initial epoch-1 snapshot), re-emitted the live rows at epoch 2, and closed
	// with a refresh-boundary marker at epoch 2 — so a keyed sink sweeps any row
	// still at epoch 1 (a row deleted at the source in the lost window, which the
	// upsert-only re-snapshot cannot signal). The row-level sweep itself is
	// covered by the syncable dialect docker test.
	require.Equal(t, uint64(2), markerEpoch, "gap-recovery re-snapshot must emit a refresh-boundary marker at the bumped epoch")
	require.Equal(t, uint64(2), genByKey["gap"], "a re-snapshotted row carries the bumped epoch")
	require.Equal(t, uint64(2), genByKey["before"], "a re-snapshotted row carries the bumped epoch")

	select {
	case err := <-ingestErr:
		require.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}
