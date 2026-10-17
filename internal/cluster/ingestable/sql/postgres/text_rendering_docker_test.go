//go:build docker || integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// gateLevel is a FeatureReader for a cluster whose minimum feature level is
// fixed: every level up to it is enabled.
type gateLevel uint64

func (g gateLevel) FeatureEnabled(level uint64) bool { return level <= uint64(g) }

// positionRendering decodes a checkpoint's text rendering and whether it is
// a streaming checkpoint (an LSN, no snapshot progress).
func positionRendering(t *testing.T, pos cluster.Position) (rendering uint32, streaming bool) {
	t.Helper()
	if len(pos) < 2 || pos[0] != 0xff {
		return 0, false
	}
	pp := &dialectpb.PostgresPosition{}
	require.NoError(t, proto.Unmarshal(pos[1:], pp))
	return pp.TextRendering, pp.Lsn > 0 && pp.SnapshotProgress == nil
}

// TestPostgresOutputRenderingMatchesCDC pins snapshot/CDC parity for every
// type whose `::text` cast is a function cast rather than the output
// function — boolean, char(n), inet, cidr, name, "char" — and a domain over
// boolean, each as a key column AND a payload column: the snapshot keys a row
// exactly as the stream's update and delete of it do, and its payload bytes
// equal the update's.
func TestPostgresOutputRenderingMatchesCDC(t *testing.T) {
	const table = "pgtest_textrender"
	const slot, pub = "slot_textrender", "pub_textrender"
	db := createDB(t)
	defer db.Close()
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS ` + table,
		`DROP DOMAIN IF EXISTS pgtest_dbool`,
		`CREATE DOMAIN pgtest_dbool AS boolean`,
		`CREATE TABLE ` + table + ` (
			id int, kb boolean, kc char(4), ki inet, kn cidr, knm name, kch "char", kdb pgtest_dbool,
			pb boolean, pc char(4), pi inet, pn cidr, pnm name, pch "char", pdb pgtest_dbool, n int,
			PRIMARY KEY (id, kb, kc, ki, kn, knm, kch, kdb))`,
		`INSERT INTO ` + table + ` VALUES (1, true, 'ab', '10.0.0.1', '10.0.0.0/8', 'nm', 'x', false,
			true, 'ab', '10.0.0.1', '10.0.0.0/8', 'nm', 'x', false, 1)`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + table)
		_, _ = db.Exec(`DROP DOMAIN IF EXISTS pgtest_dbool`)
	})
	cleanReplication(t, slot, pub)

	key := []string{"id", "kb", "kc", "ki", "kn", "knm", "kch", "kdb"}
	cols := append(append([]string{}, key...), "pb", "pc", "pi", "pn", "pnm", "pch", "pdb", "n")
	mappings := make([]sql.Mapping, len(cols))
	for i, c := range cols {
		mappings[i] = sql.Mapping{JsonName: c, SQLColumn: c}
	}
	config := &sql.Config{
		Type:             &cluster.Type{ID: "pgtextrender", Name: "pgtextrender"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       key,
		Mappings:         mappings,
		Options:          sql.Options{SlotName: slot, Publication: pub},
	}
	wantKey := sql.CompositeKey(map[string]any{
		"id": "1", "kb": "t", "kc": "ab  ", "ki": "10.0.0.1", "kn": "10.0.0.0/8", "knm": "nm", "kch": "x", "kdb": "f",
	}, key)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, 0, pr, po) }()

	snap := ingesttest.Await(t, pr, po, 60*time.Second, nil, wantKey).Entity(wantKey)
	require.NotNil(t, snap, "the snapshot keys the row as the stream does")
	var payload map[string]any
	require.NoError(t, json.Unmarshal(snap.Data, &payload))
	require.Equal(t, "ab  ", payload["pc"], "char(n) keeps its padding, as pgoutput sends it")
	require.Equal(t, "10.0.0.1", payload["pi"], "a host inet has no /32, as pgoutput sends it")
	require.Equal(t, true, payload["pb"], "a boolean payload is a JSON bool")

	waitForSlot(t, slot)
	_, err := db.Exec(`UPDATE ` + table + ` SET n = n WHERE id = 1`)
	require.NoError(t, err)
	update := awaitCommit(t, pr, po, 30*time.Second, wantKey).Entity(wantKey)
	require.NotNil(t, update, "the stream's update keys the row as the snapshot did")
	require.Equal(t, string(snap.Data), string(update.Data), "the update's payload bytes equal the snapshot's")

	_, err = db.Exec(`DELETE FROM ` + table + ` WHERE id = 1`)
	require.NoError(t, err)
	deleted := awaitCommit(t, pr, po, 30*time.Second, wantKey).Entity(wantKey)
	require.NotNil(t, deleted)
	require.True(t, deleted.IsDelete(), "the delete keys the snapshotted row, so it removes the destination row")
}

// TestPostgresTextRenderingUpgrade pins the feature-level-4 transition on a
// table keyed by a boolean (the Marten archived-stream shape). Run 1 (minimum
// level 3 — a 0.8.2/0.8.3 mixed cluster) snapshots with the `::text` cast.
// Run 2 (level 4) resuming that checkpoint re-snapshots once at a bumped
// epoch, keys by output function, and closes with the marker that sweeps the
// cast-keyed rows on keyed destinations.
func TestPostgresTextRenderingUpgrade(t *testing.T) {
	const table = "pgtest_textrender_upgrade"
	const slot, pub = "slot_textrender_upgrade", "pub_textrender_upgrade"
	db := createDB(t)
	defer db.Close()
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS ` + table,
		`CREATE TABLE ` + table + ` (seq_id int, is_archived boolean, data text, PRIMARY KEY (seq_id, is_archived))`,
		`INSERT INTO ` + table + ` VALUES (1, false, 'a')`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS ` + table) })
	cleanReplication(t, slot, pub)

	config := &sql.Config{
		Type:             &cluster.Type{ID: "pgtextupgrade", Name: "pgtextupgrade"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"seq_id", "is_archived"},
		Mappings: []sql.Mapping{
			{JsonName: "seq_id", SQLColumn: "seq_id"},
			{JsonName: "is_archived", SQLColumn: "is_archived"},
			{JsonName: "data", SQLColumn: "data"},
		},
		Options: sql.Options{SlotName: slot, Publication: pub},
	}
	keyOf := func(seq, archived string) string {
		return sql.CompositeKey(map[string]any{"seq_id": seq, "is_archived": archived}, config.PrimaryKey)
	}

	// Run 1: minimum level 3 — the cast rendering, then one streamed row so
	// the checkpoint is a streaming one.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() {
		_ = (&postgres.PostgreSQLDialect{Features: gateLevel(3)}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	ingesttest.Await(t, pr1, po1, 60*time.Second, nil, keyOf("1", "false"))
	waitForSlot(t, slot)
	_, err := db.Exec(`INSERT INTO ` + table + ` VALUES (2, true, 'b')`)
	require.NoError(t, err)
	run1 := awaitCommit(t, pr1, po1, 30*time.Second, keyOf("2", "t"))
	cancel1()
	rendering, _ := positionRendering(t, run1.Position)
	require.Zero(t, rendering, "below level 4 the checkpoint records the cast rendering")
	legacyCheckpoint := run1.Position
	require.Eventually(t, func() bool {
		var active bool
		return db.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active) == nil && !active
	}, 10*time.Second, 100*time.Millisecond, "run 1 released the slot")

	// Run 2: level 4, resuming the cast checkpoint — re-key once.
	core, observed := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() {
		_ = (&postgres.PostgreSQLDialect{Features: gateLevel(4)}).Ingest(ctx2, config, legacyCheckpoint, 0, pr2, po2)
	}()
	completed := func(pos cluster.Position) bool {
		r, streaming := positionRendering(t, pos)
		return streaming && r == 1
	}
	res := ingesttest.AwaitRefresh(t, pr2, po2, 90*time.Second, completed, keyOf("1", "f"), keyOf("2", "t"))
	restore()
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			require.NotEqual(t, keyOf("1", "false"), string(e.Key), "level 4: no row may keep the cast key")
		}
	}
	rekeyed := res.Entity(keyOf("1", "f"))
	require.Equal(t, uint64(2), rekeyed.Generation, "the re-key is a bumped-epoch re-snapshot")
	require.Equal(t, uint64(2), res.MarkerEpoch, "and closes with the marker that sweeps the cast-keyed rows on keyed destinations")
	require.NotEmpty(t, observed.FilterMessageSnippet("snapshot text rendering changed").All(), "the re-key is announced")
}

// A cast-rendered checkpoint whose tables have no column the two renderings
// spell differently (a boolean outside the key decodes to a JSON bool either
// way) is promoted without a re-snapshot: the next checkpoint records it.
func TestPostgresTextRenderingUpgradeSkipsUnaffectedTables(t *testing.T) {
	const table = "pgtest_textrender_noop"
	const slot, pub = "slot_textrender_noop", "pub_textrender_noop"
	db := createDB(t)
	defer db.Close()
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS ` + table,
		`CREATE TABLE ` + table + ` (id int PRIMARY KEY, flag boolean, data text)`,
		`INSERT INTO ` + table + ` VALUES (1, true, 'a')`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS ` + table) })
	cleanReplication(t, slot, pub)
	config := &sql.Config{
		Type:             &cluster.Type{ID: "pgtextnoop", Name: "pgtextnoop"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"id"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "flag", SQLColumn: "flag"},
			{JsonName: "data", SQLColumn: "data"},
		},
		Options: sql.Options{SlotName: slot, Publication: pub},
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() {
		_ = (&postgres.PostgreSQLDialect{Features: gateLevel(3)}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	ingesttest.Await(t, pr1, po1, 60*time.Second, nil, "1")
	waitForSlot(t, slot)
	_, err := db.Exec(`INSERT INTO ` + table + ` VALUES (2, false, 'b')`)
	require.NoError(t, err)
	run1 := awaitCommit(t, pr1, po1, 30*time.Second, "2")
	cancel1()
	require.Eventually(t, func() bool {
		var active bool
		return db.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active) == nil && !active
	}, 10*time.Second, 100*time.Millisecond, "run 1 released the slot")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() {
		_ = (&postgres.PostgreSQLDialect{Features: gateLevel(4)}).Ingest(ctx2, config, run1.Position, 0, pr2, po2)
	}()
	waitForSlot(t, slot)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES (3, true, 'c')`)
	require.NoError(t, err)
	res := ingesttest.Await(t, pr2, po2, 30*time.Second, func(pos cluster.Position) bool {
		r, streaming := positionRendering(t, pos)
		return streaming && r == 1
	}, "3")
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			require.False(t, e.IsRefreshBoundary(), "no column differs, so no re-snapshot")
			require.NotEqual(t, "1", string(e.Key), "no re-snapshot re-emits row 1")
		}
	}
}
