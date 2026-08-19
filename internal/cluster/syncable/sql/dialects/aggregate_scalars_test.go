package dialects_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

func scalarSpec() sql.AggregateSpec {
	return sql.AggregateSpec{
		Table: "jobs", PrimaryKey: "job_id", Column: "visits", Sidecar: "jobs__agg_visits",
		Scalars: []sql.AggregateScalar{
			{Column: "visit_count", Fn: "count"},
			{Column: "hours_sum", Fn: "sum", Of: "hours"},
			{Column: "last_date", Fn: "max", Of: "date"}, // text max: ISO dates
			{Column: "open_count", Fn: "count", Where: []sql.AggregateScalarWhere{{Field: "status", Equals: "open"}}},
		},
	}
}

// Scalars extend materialize/rebuild with one fold subquery per column —
// count over rows, numeric-cast sum, lexical max, and a filtered count
// whose where value compares in the ->> text space as an escaped literal.
func TestAggregateScalarSQL(t *testing.T) {
	pg := (&dialects.PostgreSQLDialect{}).CreateAggregateMaterializeSQL(scalarSpec())
	require.Contains(t, pg, `"visit_count"`)
	require.Contains(t, pg, "(SELECT COUNT(*) FROM \"jobs__agg_visits\" s WHERE s.parent_key = $3)")
	require.Contains(t, pg, "SUM((s.element->>'hours')::numeric)")
	require.Contains(t, pg, "MAX(s.element->>'date')")
	require.Contains(t, pg, "s.element->>'status' = 'open'")
	require.Equal(t, 6, strings.Count(pg, "$"), "one placeholder per value column plus the key")

	my := (&dialects.MySQLDialect{}).CreateAggregateRebuildSQL(scalarSpec())
	require.Contains(t, my, "SUM(CAST(s.element->>'$.hours' AS DECIMAL(65,10)))")
	require.Contains(t, my, "s.element->>'$.status' = 'open'")
	require.Equal(t, 6, strings.Count(my, "?"), "one placeholder per value column plus the WHERE")
}

// The no-scalar spec renders byte-identically to the pre-scalar statements —
// existing deployments' prepared SQL must not change shape.
func TestAggregateScalarSQLByteCompat(t *testing.T) {
	spec := sql.AggregateSpec{Table: "jobs", PrimaryKey: "job_id", Column: "visits", Sidecar: "jobs__agg_visits"}
	pg := (&dialects.PostgreSQLDialect{}).CreateAggregateMaterializeSQL(spec)
	require.True(t, strings.HasPrefix(pg, `INSERT INTO "jobs" ("job_id","visits") VALUES ($1,`), pg)
	require.True(t, strings.HasSuffix(pg, `ON CONFLICT ("job_id") DO UPDATE SET "visits"=EXCLUDED."visits"`), pg)
	my := (&dialects.MySQLDialect{}).CreateAggregateRebuildSQL(spec)
	require.True(t, strings.HasPrefix(my, "UPDATE `jobs` SET `visits`="), my)
	require.True(t, strings.HasSuffix(my, "WHERE `job_id`=?"), my)
}

// A scalars-only aggregate (no array column) renders without the array
// subquery and with correspondingly fewer placeholders.
func TestAggregateScalarsOnlySQL(t *testing.T) {
	spec := sql.AggregateSpec{
		Table: "jobs", PrimaryKey: "job_id", Sidecar: "jobs__agg_visit_count",
		Scalars: []sql.AggregateScalar{{Column: "visit_count", Fn: "count"}},
	}
	pg := (&dialects.PostgreSQLDialect{}).CreateAggregateMaterializeSQL(spec)
	require.NotContains(t, pg, "jsonb_agg")
	require.Equal(t, 2, strings.Count(pg, "$"))
	my := (&dialects.MySQLDialect{}).CreateAggregateMaterializeSQL(spec)
	require.NotContains(t, my, "JSON_ARRAYAGG")
	require.Equal(t, 2, strings.Count(my, "?"))
}
