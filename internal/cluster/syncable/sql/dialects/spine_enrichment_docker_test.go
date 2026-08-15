//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// spineEnrichmentFlow drives scalar lookup enrichment end to end against a
// real destination, gated at every step by THE JOIN-EQUIVALENCE ORACLE: the
// projected table must equal what the reference join query returns right now
// (Phil's invariant — "the standard is what would happen if I ran the query
// with joins"). Steps: dimension-first resolve, spine-first NULL + heal on
// dimension arrival, dimension rename fan-out, dimension delete → NULL —
// with an INT on column, the canonical-join-space case the design exists for.
func spineEnrichmentFlow(t *testing.T, db *sql.DB, d sql.Dialect, table string, ph func(i int) string) {
	config := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(32)"},
			{Name: "tenant_id", SQLType: "INT"},
			{Name: "tenant_name", SQLType: "VARCHAR(64)"},
		},
		Sources: []sql.ProjectionSource{
			{
				Topic:    "job",
				KeyPath:  []string{"$.id"},
				OnDelete: "delete-row",
				Rules: []sql.ProjectionRule{{
					When: []sql.WhenClause{{Path: "$.kind", Equals: "job"}},
					Set: []sql.ProjectionSet{
						{Column: "tenant_id", From: "$.tenant"},
						{Column: "tenant_name", Lookup: "tenants", On: "tenant_id", Select: "name"},
					},
				}},
			},
			{
				Topic: "tenant",
				Lookup: &sql.ProjectionLookup{
					Name: "tenants",
					Fields: []sql.ProjectionElementField{
						{Field: "name", From: "$.name"},
					},
				},
			},
		},
	}

	proj := sql.NewProjection(db, config, nil, "spine-test")
	require.NoError(t, proj.Init(), "enriched projection Init (DDL + dimension + index + prepares)")
	defer proj.Close()

	jobType := &cluster.Type{ID: "job", Name: "job"}
	tenantType := &cluster.Type{ID: "tenant", Name: "tenant"}
	apply := func(typ *cluster.Type, key, payload string) {
		t.Helper()
		_, err := proj.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(typ, []byte(key), []byte(payload)),
		}})
		require.NoError(t, err)
	}

	// THE ORACLE: the projected row's tenant_name must equal the live
	// reference join at every gate.
	dim := table + "__lookup_tenants"
	assertEquivalence := func(msg string) {
		t.Helper()
		// MySQL: numeric-side join (string-side CAST trips a collation mix
		// between the dimension column and the cast expression).
		q := fmt.Sprintf(`SELECT COUNT(*) FROM %s p LEFT JOIN %s d ON CAST(d.lookup_key AS SIGNED) = p.tenant_id WHERE (p.tenant_name IS NULL) <> (d.lookup_key IS NULL)`, table, dim)
		if _, isPG := d.(*dialects.PostgreSQLDialect); isPG {
			q = fmt.Sprintf(`SELECT COUNT(*) FROM %s p LEFT JOIN %s d ON d.lookup_key = p.tenant_id::text WHERE (p.tenant_name IS NULL) <> (d.lookup_key IS NULL)`, table, dim)
		}
		var divergent int
		require.NoError(t, db.DB.QueryRow(q).Scan(&divergent))
		require.Zero(t, divergent, "join-equivalence violated (%s): projected NULL-ness diverges from the live reference join", msg)
	}
	tenantName := func(jobID string) any {
		t.Helper()
		var v gosql.NullString
		q := fmt.Sprintf("SELECT tenant_name FROM %s WHERE job_id = %s", table, ph(0))
		require.NoError(t, db.DB.QueryRow(q, jobID).Scan(&v))
		if !v.Valid {
			return nil
		}
		return v.String
	}

	// 1. Dimension first: the job resolves at apply.
	apply(tenantType, "7", `{"name":"Acme"}`)
	apply(jobType, "j1", `{"kind":"job","id":"j1","tenant":7}`)
	require.Equal(t, "Acme", tenantName("j1"), "apply-time resolve (dimension existed)")
	assertEquivalence("after dimension-first apply")

	// 2. Spine first: NULL now (LEFT JOIN semantics), healed when the
	// dimension row arrives.
	apply(jobType, "j2", `{"kind":"job","id":"j2","tenant":9}`)
	require.Nil(t, tenantName("j2"), "spine-before-dimension is NULL — the LEFT JOIN answer")
	assertEquivalence("spine-first, dimension absent")
	apply(tenantType, "9", `{"name":"Globex"}`)
	require.Equal(t, "Globex", tenantName("j2"), "dimension arrival heals via fan-out")
	assertEquivalence("after heal")

	// 3. Rename fan-out: every referencing row updates.
	apply(jobType, "j3", `{"kind":"job","id":"j3","tenant":7}`)
	apply(tenantType, "7", `{"name":"Acme Corp"}`)
	require.Equal(t, "Acme Corp", tenantName("j1"), "rename fans out to row 1")
	require.Equal(t, "Acme Corp", tenantName("j3"), "rename fans out to row 2")
	require.Equal(t, "Globex", tenantName("j2"), "unrelated tenant untouched")
	assertEquivalence("after rename fan-out")

	// 4. Dimension delete: referencing rows go NULL (what the join would say).
	_, err := proj.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(tenantType, []byte("7")),
	}})
	require.NoError(t, err)
	require.Nil(t, tenantName("j1"), "dimension delete NULLs the enriched column")
	require.Equal(t, "Globex", tenantName("j2"), "other dimension rows unaffected")
	assertEquivalence("after dimension delete")
}

func TestPostgreSQLIntegration_SpineEnrichmentJoinEquivalence(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	// Short unique name: the dimension suffix (__lookup_tenants) must fit
	// inside MySQL's 64-char identifier limit and Postgres's 63-byte silent
	// truncation — the full test-derived name does not.
	table := fmt.Sprintf("spine_pg_%d", time.Now().UnixNano()%1e9)
	defer dropTable(t, table)
	defer dropTable(t, table+"__lookup_tenants")

	spineEnrichmentFlow(t, db, d, table, func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_SpineEnrichmentJoinEquivalence(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := fmt.Sprintf("spine_my_%d", time.Now().UnixNano()%1e9)
	defer dropTableMySQL(t, db, table)
	defer dropTableMySQL(t, db, table+"__lookup_tenants")

	spineEnrichmentFlow(t, db, d, table, func(int) string { return "?" })
}
