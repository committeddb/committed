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
					"-c", "max_replication_slots=10",
					"-c", "max_wal_senders=10",
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
