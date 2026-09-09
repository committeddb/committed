//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/sqlident"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// The destination reference. Committed decides many details of the rows it
// writes into your database — how a UUID or a decimal is rendered, the order
// of an array column's elements, the shape of the helper tables it keeps
// beside yours. None of that is in the config. If a release changed one of
// those details without saying so, rows written before the upgrade and rows
// written after would sit in one table rendered two ways, and nothing would
// notice. So every destination carries a note (committed__sink_meta) saying
// which rendering version wrote its rows, and this test pins what that
// version means: it folds every sink in the fixture through the production
// Sync path into a real database, dumps every table committed created plus
// the notes, and compares the dump against a reference chosen by the version
// read back from the database. A rendering change fails under the current
// number; bumping sql.SinkRenderingVersion demands a new reference generated on
// purpose:
//
//	UPDATE_SINK_REFERENCE=1 go test -tags docker ./internal/cluster/syncable/sql/dialects -run TestSinkRenderingReference
//
// and committed with the bump. The bump is what makes a worker refuse to
// write into rows rendered by the older version (db/rendering_stamp.go).
func TestSinkRenderingReference_Postgres(t *testing.T) {
	runSinkReference(t, "postgres", &dialects.PostgreSQLDialect{}, pgConnString, "JSONB", "DOUBLE PRECISION", destinationCatalog{
		tables:  "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE $1 ORDER BY table_name",
		columns: "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position",
		orderBy: func(col, dataType string) string {
			if strings.Contains(dataType, "json") {
				return col + "::text"
			}
			return col
		},
		meta:  "SELECT table_name, rendering_version FROM " + sqlident.Postgres.Table(sql.SinkMetaTable) + " WHERE table_name LIKE $1 ORDER BY table_name",
		quote: sqlident.Postgres.Table,
	})
}

func TestSinkRenderingReference_MySQL(t *testing.T) {
	runSinkReference(t, "mysql", &dialects.MySQLDialect{}, mysqlConn(t), "JSON", "DOUBLE", destinationCatalog{
		tables:  "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ? ORDER BY table_name",
		columns: "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position",
		orderBy: func(col, dataType string) string {
			if strings.Contains(dataType, "json") {
				return "CAST(" + col + " AS CHAR)"
			}
			return col
		},
		meta:  "SELECT table_name, rendering_version FROM " + sqlident.MySQL.Table(sql.SinkMetaTable) + " WHERE table_name LIKE ? ORDER BY table_name",
		quote: sqlident.MySQL.Table,
	})
}

type destinationCatalog struct {
	tables, columns, meta string
	quote                 func(string) string
	// orderBy renders one column for the dump's ORDER BY: JSON columns are
	// cast to text so every dialect can sort them.
	orderBy func(col, dataType string) string
}

func runSinkReference(t *testing.T, name string, d sql.Dialect, conn, jsonType, floatType string, cat destinationCatalog) {
	t.Helper()
	// A short prefix: MySQL caps identifiers at 64 bytes and the helper
	// tables append suffixes to the projected table's name.
	u := strings.ToLower(uniqueTable(t))
	prefix := "sg" + u[len(u)-8:]
	db, err := sql.NewDB(d, conn)
	require.NoError(t, err)
	defer db.Close()
	fx := sinkReferenceFixture(prefix, jsonType, floatType)
	ctx := context.Background()

	hist := sql.New(db, fx.hist)
	keyed := sql.New(db, fx.keyed)
	values := sql.New(db, fx.values)
	single := sql.NewProjection(db, fx.single, nil, fx.single.Table)
	movies := sql.NewProjection(db, fx.movies, nil, fx.movies.Table)
	jobs := sql.NewProjection(db, fx.jobs, nil, fx.jobs.Table)
	jobs.SetStoreDir(t.TempDir())
	items := sql.NewProjection(db, fx.items, nil, fx.items.Table)
	for _, s := range []interface{ Init() error }{hist, keyed, values, single, movies, jobs, items} {
		require.NoError(t, s.Init())
	}
	defer func() {
		for _, s := range []interface{ Teardown() error }{hist, keyed, values, single, movies, jobs, items} {
			_ = s.Teardown()
		}
	}()

	sinkReferenceFeed(t, ctx, hist, keyed, values, single, movies, jobs, items)

	// The worker stamps on first contact; here the sinks stamp themselves so
	// the notes are part of the dump.
	for _, s := range []interface {
		StampRendering(context.Context) error
	}{hist, keyed, values, single, movies, jobs, items} {
		require.NoError(t, s.StampRendering(ctx))
	}

	dump, version := dumpDestination(t, db.DB, cat, prefix)
	dump = strings.ReplaceAll(dump, prefix, "T")
	referencePath := filepath.Join("testdata", fmt.Sprintf("sink_rendering_%s_%d.reference", name, version))
	if os.Getenv("UPDATE_SINK_REFERENCE") == "1" {
		require.NoError(t, os.WriteFile(referencePath, []byte(dump), 0o644))
		t.Logf("wrote %s", referencePath)
	}
	want, err := os.ReadFile(referencePath)
	require.NoError(t, err, "no reference for sink rendering version %d on %s: the version was bumped — generate its reference deliberately (UPDATE_SINK_REFERENCE=1) and commit it with the bump", version, name)
	require.Equal(t, string(want), dump,
		"the %s destination's bytes changed under rendering version %d. Rows already in customers' tables were rendered the old way; bump sql.SinkRenderingVersion (so their workers park until rematerialized) and regenerate the reference for the new number", name, version)
}

// sinkReferenceFeed folds a deterministic event set through every sink: keyless
// appends with a replayed duplicate, a keyed upsert and delete, the tenant
// lifecycle, a movie with a cast whose names come from a lookup, and a job
// with a spine-enriched tenant, fanned items, and a stage-folded total.
func sinkReferenceFeed(t *testing.T, ctx context.Context, hist, keyed, values *sql.Syncable, single, movies, jobs, items *sql.Projection) {
	t.Helper()
	var idx uint64
	fold := func(s interface {
		Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error)
	}, topic, key string, fields map[string]any,
	) {
		t.Helper()
		idx++
		_, err := s.Sync(ctx, sinkEvent(t, idx, topic, key, fields))
		require.NoError(t, err)
	}
	del := func(s interface {
		Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error)
	}, topic, key string,
	) {
		t.Helper()
		idx++
		_, err := s.Sync(ctx, sinkDelete(idx, topic, key))
		require.NoError(t, err)
	}
	fold(hist, "audit", "a1", map[string]any{"actor": "ada", "action": "login", "amount": "2.50"})
	fold(hist, "audit", "a2", map[string]any{"actor": "bea", "action": "pay", "amount": 10})
	replayed := sinkEvent(t, idx, "audit", "a2", map[string]any{"actor": "bea", "action": "pay", "amount": 10})
	_, err := hist.Sync(ctx, replayed) // same index again: the applied log dedups it
	require.NoError(t, err)

	fold(keyed, "account", "k1", map[string]any{"id": "k1", "owner": "ada", "balance": "5.0000", "tags": []any{"b", "a"}})
	fold(keyed, "account", "k2", map[string]any{"id": "k2", "owner": "bea", "balance": 7, "tags": map[string]any{"vip": true}})
	fold(keyed, "account", "k1", map[string]any{"id": "k1", "owner": "ada", "balance": "5.25", "tags": []any{"a"}})
	del(keyed, "account", "k2")

	for _, row := range sinkValueRows() {
		fold(values, "values", row["id"].(string), row)
	}

	fold(single, "tenant-event", "e1", map[string]any{"tenant_id": "t1", "event_type": "created", "tier": "dev", "seats": 3})
	fold(single, "tenant-event", "e2", map[string]any{"tenant_id": "t2", "event_type": "created", "tier": "prod", "seats": 10})
	fold(single, "tenant-event", "e3", map[string]any{"tenant_id": "t1", "event_type": "provisioned", "allocs": map[string]any{"cpu": 4, "gpu": nil}})
	fold(single, "tenant-event", "e4", map[string]any{"tenant_id": "t2", "event_type": "deprovisioned"})

	fold(movies, "name", "nm1", map[string]any{"primary_name": "Ada Lovelace"})
	fold(movies, "name", "nm2", map[string]any{"primary_name": "Bea Arthur"})
	fold(movies, "title", "tt1", map[string]any{"tconst": "tt1", "kind": "movie", "primary_title": "The Engine"})
	fold(movies, "title", "tt2", map[string]any{"tconst": "tt2", "kind": "short", "primary_title": "Skipped"})
	fold(movies, "title", "tt3", map[string]any{"tconst": "tt3", "kind": "movie", "primary_title": "Deleted Later"})
	fold(movies, "principal", "p1", map[string]any{"tconst": "TT1", "ordering": 2, "nconst": "nm2", "category": "actor", "nick": "B"})
	fold(movies, "principal", "p2", map[string]any{"tconst": "tt1", "ordering": 1, "nconst": "nm1", "category": "actor", "nick": nil})
	fold(movies, "principal", "p3", map[string]any{"tconst": "tt1", "ordering": 10, "nconst": "nm9", "category": "director", "nick": "D"})
	del(movies, "principal", "p3")
	del(movies, "title", "tt3") // the row owner's delete-row removes the row

	fold(jobs, "tenant", "7", map[string]any{"name": "Acme"})
	fold(jobs, "job", "j1", map[string]any{"id": "j1", "kind": "job", "tenant": 7})
	fold(jobs, "job", "j2", map[string]any{"id": "j2", "kind": "note", "tenant": 7})
	fold(jobs, "job", "j3", map[string]any{"id": "j3", "kind": "job", "tenant": 7})
	del(jobs, "job", "j3") // delete-row on the owning source
	x1 := map[string]any{"id": "x1", "txn": "x1", "job": "j1", "total": "12.50", "items": []any{
		map[string]any{"id": "j1-a", "amount": "2.50"}, map[string]any{"id": "j1-b", "amount": "10"},
	}}
	x2 := map[string]any{"id": "x2", "txn": "x2", "job": "j1", "total": "1.25", "items": []any{}}
	fold(jobs, "txn", "x1", x1)
	fold(jobs, "txn", "x2", x2)
	fold(items, "txn", "x1", x1)
	fold(items, "txn", "x2", x2)
	fold(items, "txn", "x1", map[string]any{"id": "x1", "txn": "x1", "items": []any{map[string]any{"id": "j1-a", "amount": "3.00"}}}) // j1-b reconciled away
}

// dumpDestination renders every table committed created under prefix (the
// projected tables and their helper tables), each row ordered by every
// column, plus the rendering notes for those tables; it returns the version
// the notes carry.
func dumpDestination(t *testing.T, db *gosql.DB, cat destinationCatalog, prefix string) (string, uint64) {
	t.Helper()
	var sb strings.Builder
	rows, err := db.Query(cat.tables, prefix+"%")
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		tables = append(tables, n)
	}
	require.NoError(t, rows.Close())
	for _, table := range tables {
		cols, err := db.Query(cat.columns, table)
		require.NoError(t, err)
		var names, types []string
		for cols.Next() {
			var n, dt string
			require.NoError(t, cols.Scan(&n, &dt))
			names = append(names, n)
			types = append(types, strings.ToLower(dt))
		}
		require.NoError(t, cols.Close())
		fmt.Fprintf(&sb, "== %s (%s)\n", table, strings.Join(names, ", "))
		quoted := make([]string, len(names))
		order := make([]string, len(names))
		for i, n := range names {
			quoted[i] = cat.quote(n)
			order[i] = cat.orderBy(cat.quote(n), types[i])
		}
		q := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", strings.Join(quoted, ", "), cat.quote(table), strings.Join(order, ", "))
		data, err := db.Query(q)
		require.NoError(t, err, q)
		for data.Next() {
			vals := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, data.Scan(ptrs...))
			out := make([]string, len(vals))
			for i, v := range vals {
				out[i] = renderCell(v)
			}
			sb.WriteString(strings.Join(out, "\t") + "\n")
		}
		require.NoError(t, data.Close())
	}
	sb.WriteString("== " + sql.SinkMetaTable + " (table_name, rendering_version)\n")
	meta, err := db.Query(cat.meta, prefix+"%")
	require.NoError(t, err)
	var version uint64
	for meta.Next() {
		var table string
		var v int64
		require.NoError(t, meta.Scan(&table, &v))
		version = uint64(v)
		fmt.Fprintf(&sb, "%s\t%d\n", table, v)
	}
	require.NoError(t, meta.Close())
	require.NotZero(t, version, "the destination carries no rendering note")
	return sb.String(), version
}

func renderCell(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return strconv.Quote(string(x))
	case string:
		return strconv.Quote(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(x)
	}
}
