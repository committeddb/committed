//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// MySQL twin of postgres_docker_projection_test.go — the same five projection
// scenarios (lifecycle, multi-source, aggregate split, re-parenting, lookup
// enrichment), scenario for scenario, against a real MySQL. Deliberate fixture
// translations, asserted as equivalences rather than skipped:
//
//   - JSONB → JSON.
//   - Postgres NUMERIC (arbitrary precision) → explicit-scale DECIMAL: bare
//     MySQL NUMERIC is DECIMAL(10,0), which would silently truncate 9.3 → 9 —
//     exactly the class of dialect surprise this suite exists to catch.
//   - $1 placeholders → ?.
//   - Table teardown goes through the MySQL dialect's own DropDDL
//     (dropTableMySQLByName) since the shared dropTable helper is
//     Postgres-backed.
//
// The JSON assertions unmarshal before comparing (MySQL normalizes JSON
// key order/spacing; element ORDER inside arrays is load-bearing and IS
// asserted — it proves the dialect's JSON_ARRAYAGG ordered-derived-table
// workaround).

// dropTableMySQLByName drops a table on the shared MySQL container with a
// fresh connection, so cleanup runs even if the test's connection failed.
func dropTableMySQLByName(t *testing.T, table string) {
	t.Helper()
	d := &dialects.MySQLDialect{}
	db, err := d.Open(mysqlConn(t))
	if err != nil {
		t.Logf("dropTableMySQLByName: open: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(d.DropDDL(&sql.Config{Table: table})); err != nil {
		t.Logf("dropTableMySQLByName %s: %v", table, err)
	}
}

func mysqlProjectionConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      table,
		PrimaryKey: "tenant_id",
		Columns: []sql.ProjectionColumn{
			{Name: "tenant_id", SQLType: "VARCHAR(64)"},
			{Name: "tier", SQLType: "VARCHAR(32)"},
			{Name: "state", SQLType: "VARCHAR(32)"},
			{Name: "allocs", SQLType: "JSON"},
		},
		Rules: []sql.ProjectionRule{
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.created"}},
				Set: []sql.ProjectionSet{
					{Column: "tier", From: "$.tier"},
					{Column: "state", Value: "pending"},
				},
			},
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.provisioned"}},
				Set: []sql.ProjectionSet{
					{Column: "state", Value: "active"},
					{Column: "allocs", From: "$.allocs"},
				},
			},
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.deprovisioned"}},
				Set: []sql.ProjectionSet{
					{Column: "state", Value: "deprovisioning"},
				},
			},
		},
	}
}

// TestMySQLIntegration_ProjectionLifecycle mirrors the Postgres lifecycle
// criterion on real MySQL: the tenant lifecycle folds to one converged row per
// tenant (ON DUPLICATE KEY upserts restricted to each rule's columns),
// replaying everything twice over reproduces identical state, unmatched events
// leave no row, and delete Actuals hard-delete.
func TestMySQLIntegration_ProjectionLifecycle(t *testing.T) {
	table := uniqueTable(t)
	defer dropTableMySQLByName(t, table)

	db, err := sql.NewDB(&dialects.MySQLDialect{}, mysqlConn(t))
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, mysqlProjectionConfig(table), nil, "tenants")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	lifecycle := []*cluster.Actual{
		projectionEvent(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.created", "tier": "dev"}),
		projectionEvent(t, "t2", map[string]any{"tenant_id": "t2", "event_type": "tenant.created", "tier": "prod"}),
		projectionEvent(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.provisioned", "allocs": map[string]any{"cpu": 4}}),
		projectionEvent(t, "t3", map[string]any{"tenant_id": "t3", "event_type": "tenant.billed"}), // no rule
		projectionEvent(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.deprovisioned"}),
	}

	replay := func() {
		for _, a := range lifecycle {
			_, err := projection.Sync(ctx, a)
			require.Nil(t, err)
		}
	}

	type row struct {
		Tier   string
		State  string
		Allocs any
	}
	readRows := func() map[string]row {
		rows, err := db.DB.Query("SELECT tenant_id, tier, state, allocs FROM " + mysqlBacktick(table))
		require.Nil(t, err)
		defer rows.Close()
		got := map[string]row{}
		for rows.Next() {
			var id string
			var r row
			var tier, allocs gosql.NullString
			require.Nil(t, rows.Scan(&id, &tier, &r.State, &allocs))
			r.Tier = tier.String
			if allocs.Valid {
				require.Nil(t, json.Unmarshal([]byte(allocs.String), &r.Allocs))
			}
			got[id] = r
		}
		require.Nil(t, rows.Err())
		return got
	}

	want := map[string]row{
		"t1": {Tier: "dev", State: "deprovisioning", Allocs: map[string]any{"cpu": float64(4)}},
		"t2": {Tier: "prod", State: "pending"},
	}

	replay()
	require.Equal(t, want, readRows(), "fold result after first pass")

	// Replay from index 0 — twice over — must reproduce identical state.
	replay()
	replay()
	require.Equal(t, want, readRows(), "fold result after replaying twice")

	// Delete Actual hard-deletes exactly t1; a ghost delete is a no-op.
	for _, key := range []string{"t1", "ghost"} {
		_, err := projection.Sync(ctx, &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(projectionEventType, []byte(key)),
		}})
		require.Nil(t, err)
	}
	require.Equal(t, map[string]row{"t2": want["t2"]}, readRows())
}

// mysqlMovieCardConfig is movieCardConfig with MySQL types: explicit-scale
// DECIMAL where Postgres used bare NUMERIC (see the file header).
func mysqlMovieCardConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: "tconst",
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "primary_title", SQLType: "VARCHAR(255)"},
			{Name: "start_year", SQLType: "DECIMAL(10,0)"},
			{Name: "genres", SQLType: "VARCHAR(255)"},
			{Name: "average_rating", SQLType: "DECIMAL(3,1)"},
			{Name: "num_votes", SQLType: "DECIMAL(10,0)"},
		},
		Sources: []sql.ProjectionSource{
			{
				Topic:    "title",
				OnDelete: "delete-row",
				Rules: []sql.ProjectionRule{{
					Set: []sql.ProjectionSet{
						{Column: "primary_title", From: "$.primary_title"},
						{Column: "start_year", From: "$.start_year"},
						{Column: "genres", From: "$.genres"},
					},
				}},
			},
			{
				Topic:    "rating",
				OnDelete: "clear",
				Rules: []sql.ProjectionRule{{
					Set: []sql.ProjectionSet{
						{Column: "average_rating", From: "$.average_rating"},
						{Column: "num_votes", From: "$.num_votes"},
					},
				}},
			},
		},
	}
}

// TestMySQLIntegration_MultiSourceProjection mirrors the Postgres multi-source
// criterion on real MySQL: two source topics fold into one row with distinct
// columns (no clobber), a contributor's delete clears just its columns (the row
// survives), the spine's delete drops the row, and a replay-from-0 reproduces
// the fold.
func TestMySQLIntegration_MultiSourceProjection(t *testing.T) {
	table := uniqueTable(t)
	defer dropTableMySQLByName(t, table)

	db, err := sql.NewDB(&dialects.MySQLDialect{}, mysqlConn(t))
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, mysqlMovieCardConfig(table), nil, "movie_card")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	type row struct{ Title, Year, Genres, Rating, Votes gosql.NullString }
	readRow := func(tconst string) (row, bool) {
		var r row
		err := db.DB.QueryRow(
			"SELECT primary_title, start_year, genres, average_rating, num_votes FROM "+mysqlBacktick(table)+" WHERE tconst = ?", tconst,
		).Scan(&r.Title, &r.Year, &r.Genres, &r.Rating, &r.Votes)
		if err == gosql.ErrNoRows {
			return row{}, false
		}
		require.Nil(t, err)
		return r, true
	}

	title := sourceEvent(t, titleType, "tt1", map[string]any{"tconst": "tt1", "primary_title": "The Shawshank Redemption", "start_year": 1994, "genres": "Drama"})
	rating := sourceEvent(t, ratingType, "tt1", map[string]any{"tconst": "tt1", "average_rating": 9.3, "num_votes": 2800000})

	// Fold: title then rating into one row, distinct columns, no clobber.
	_, err = projection.Sync(ctx, title)
	require.Nil(t, err)
	_, err = projection.Sync(ctx, rating)
	require.Nil(t, err)

	r, ok := readRow("tt1")
	require.True(t, ok)
	require.Equal(t, "The Shawshank Redemption", r.Title.String)
	require.Equal(t, "Drama", r.Genres.String)
	require.Equal(t, "1994", r.Year.String)
	require.Equal(t, "9.3", r.Rating.String, "rating folded into the same row (DECIMAL(3,1) keeps the fraction)")
	require.Equal(t, "2800000", r.Votes.String)

	// Contributor delete (rating, onDelete=clear): rating columns NULL, title
	// columns intact, the movie row survives.
	_, err = projection.Sync(ctx, sourceDelete(ratingType, "tt1"))
	require.Nil(t, err)
	r, ok = readRow("tt1")
	require.True(t, ok, "the movie row must survive a rating delete")
	require.Equal(t, "The Shawshank Redemption", r.Title.String, "title columns untouched")
	require.False(t, r.Rating.Valid, "rating column cleared to NULL")
	require.False(t, r.Votes.Valid)

	// Spine delete (title, onDelete=delete-row): the row is dropped.
	_, err = projection.Sync(ctx, sourceDelete(titleType, "tt1"))
	require.Nil(t, err)
	_, ok = readRow("tt1")
	require.False(t, ok, "deleting the title (spine) drops the movie row")

	// Rebuild from 0: re-applying the upserts reproduces the folded row.
	_, err = projection.Sync(ctx, title)
	require.Nil(t, err)
	_, err = projection.Sync(ctx, rating)
	require.Nil(t, err)
	r, ok = readRow("tt1")
	require.True(t, ok)
	require.Equal(t, "The Shawshank Redemption", r.Title.String)
	require.Equal(t, "9.3", r.Rating.String)
}

// mysqlMovieCardSplitConfig is movieCardSplitConfig with JSON columns.
func mysqlMovieCardSplitConfig(table string) *sql.ProjectionConfig {
	castElement := []sql.ProjectionElementField{
		{Field: "ordering", From: "$.ordering"},
		{Field: "nconst", From: "$.nconst"},
	}
	return &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: "tconst",
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "primary_title", SQLType: "VARCHAR(255)"},
			{Name: "top_cast", SQLType: "JSON"},
			{Name: "directors", SQLType: "JSON"},
		},
		Sources: []sql.ProjectionSource{
			{
				Topic:    "title",
				OnDelete: "delete-row",
				Rules: []sql.ProjectionRule{{
					Set: []sql.ProjectionSet{{Column: "primary_title", From: "$.primary_title"}},
				}},
			},
			{
				Topic: "principal",
				When:  []sql.WhenClause{{Path: "$.category", Equals: "actor"}},
				Aggregate: &sql.ProjectionAggregate{
					Column:         "top_cast",
					ElementKey:     "$.ordering",
					ElementKeyType: "number",
					Element:        castElement,
				},
			},
			{
				Topic: "principal",
				When:  []sql.WhenClause{{Path: "$.category", Equals: "director"}},
				Aggregate: &sql.ProjectionAggregate{
					Column:         "directors",
					ElementKey:     "$.ordering",
					ElementKeyType: "number",
					Element:        castElement,
				},
			},
		},
	}
}

// TestMySQLIntegration_AggregateProjection mirrors the Postgres aggregate
// criterion on real MySQL. The numeric-order assertion (1,3,10 — not lexical
// 1,10,3) is the direct proof of the dialect's JSON_ARRAYAGG
// ordered-derived-table workaround: JSON_ARRAYAGG itself ignores ORDER BY.
func TestMySQLIntegration_AggregateProjection(t *testing.T) {
	const table = "agg_movie_card_my"
	dropTableMySQLByName(t, table)
	defer dropTableMySQLByName(t, table)
	defer dropTableMySQLByName(t, table+"__top_cast")
	defer dropTableMySQLByName(t, table+"__directors")

	db, err := sql.NewDB(&dialects.MySQLDialect{}, mysqlConn(t))
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, mysqlMovieCardSplitConfig(table), nil, "movie_card")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	principal := func(ordering int, nconst, category string) *cluster.Actual {
		key := fmt.Sprintf("[\"tt1\",\"%d\"]", ordering)
		return sourceEvent(t, principalType, key, map[string]any{
			"tconst": "tt1", "ordering": ordering, "nconst": nconst, "category": category,
		})
	}
	principalKey := func(ordering int) string { return fmt.Sprintf("[\"tt1\",\"%d\"]", ordering) }

	// nconstsOf reads one array column and returns its elements' nconst values
	// in stored order — the order is what proves numeric vs lexical sorting.
	nconstsOf := func(col string) []string {
		var raw gosql.NullString
		err := db.DB.QueryRow("SELECT "+mysqlBacktick(col)+" FROM "+mysqlBacktick(table)+" WHERE tconst = ?", "tt1").Scan(&raw)
		if err == gosql.ErrNoRows || !raw.Valid {
			return nil
		}
		require.NoError(t, err)
		var arr []map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw.String), &arr))
		out := make([]string, len(arr))
		for i, m := range arr {
			out[i] = m["nconst"].(string)
		}
		return out
	}
	rowExists := func() bool {
		var n int
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)+" WHERE tconst = ?", "tt1").Scan(&n))
		return n == 1
	}

	// The folding sequence: spine, then actors and a director out of order, an
	// actor with two-digit ordering (the numeric-sort probe), an actor replace,
	// and two deletes (an actor and the director).
	seq := []*cluster.Actual{
		sourceEvent(t, titleType, "tt1", map[string]any{"tconst": "tt1", "primary_title": "Heat"}),
		principal(1, "nm1", "actor"),
		principal(3, "nm3", "actor"),
		principal(2, "nmDir", "director"),
		principal(10, "nm10", "actor"),
	}
	apply := func(as ...*cluster.Actual) {
		for _, a := range as {
			_, err := projection.Sync(ctx, a)
			require.NoError(t, err)
		}
	}

	apply(seq...)
	// Numeric order: 1,3,10 — not lexical 1,10,3.
	require.Equal(t, []string{"nm1", "nm3", "nm10"}, nconstsOf("top_cast"), "actors fold into top_cast, numeric order")
	require.Equal(t, []string{"nmDir"}, nconstsOf("directors"), "the director folds into its own column")
	require.Equal(t, "Heat", func() string {
		var s string
		require.NoError(t, db.DB.QueryRow("SELECT primary_title FROM "+mysqlBacktick(table)+" WHERE tconst = ?", "tt1").Scan(&s))
		return s
	}(), "the spine column coexists with both aggregates")

	// Re-deliver ordering 1 with a new nconst: the element is replaced, not
	// duplicated, and the array stays numerically ordered.
	replace := principal(1, "nm1b", "actor")
	apply(replace)
	require.Equal(t, []string{"nm1b", "nm3", "nm10"}, nconstsOf("top_cast"))

	// Delete an actor: removed from top_cast only. The delete routes to both
	// aggregate sources, but the director source never folded it, so directors
	// is untouched — the split self-selects.
	delActor := sourceDelete(principalType, principalKey(3))
	apply(delActor)
	require.Equal(t, []string{"nm1b", "nm10"}, nconstsOf("top_cast"), "the deleted actor's element is gone")
	require.Equal(t, []string{"nmDir"}, nconstsOf("directors"), "the other column is untouched")
	require.True(t, rowExists(), "the movie row survives a child delete")

	// Delete the director: directors empties to [], the row and top_cast stay.
	delDirector := sourceDelete(principalType, principalKey(2))
	apply(delDirector)
	require.Equal(t, []string{"nm1b", "nm10"}, nconstsOf("top_cast"))
	require.Empty(t, nconstsOf("directors"), "removing the last child leaves an empty array, row intact")
	require.True(t, rowExists())

	// Capture the converged state, then rebuild from 0 (teardown + re-init drops
	// the table and sidecars and recreates them empty) and replay the whole
	// sequence: the arrays must reproduce exactly.
	wantCast := nconstsOf("top_cast")
	full := append(append([]*cluster.Actual{}, seq...), replace, delActor, delDirector)

	require.NoError(t, projection.Teardown())
	require.NoError(t, projection.Init())
	apply(full...)
	require.Equal(t, wantCast, nconstsOf("top_cast"), "rebuild-from-0 reproduces top_cast")
	require.Empty(t, nconstsOf("directors"), "rebuild-from-0 reproduces directors")
}

// mysqlDeptRosterConfig is deptRosterConfig with a JSON members column.
func mysqlDeptRosterConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: "dept",
		Columns: []sql.ProjectionColumn{
			{Name: "dept", SQLType: "VARCHAR(16)"},
			{Name: "members", SQLType: "JSON"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:    "employee",
			KeyPath:  "$.dept",
			OnDelete: "remove-from-aggregate",
			Aggregate: &sql.ProjectionAggregate{
				Column:     "members",
				ElementKey: "$.emp",
				Element:    []sql.ProjectionElementField{{Field: "emp", From: "$.emp"}},
			},
		}},
	}
}

// TestMySQLIntegration_AggregateReparenting mirrors the Postgres stale-element
// criterion on real MySQL: an employee re-delivered under a new department must
// vanish from the old department's members array, not linger in both.
func TestMySQLIntegration_AggregateReparenting(t *testing.T) {
	const table = "agg_dept_roster_my"
	dropTableMySQLByName(t, table)
	defer dropTableMySQLByName(t, table)
	defer dropTableMySQLByName(t, table+"__members")

	db, err := sql.NewDB(&dialects.MySQLDialect{}, mysqlConn(t))
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, mysqlDeptRosterConfig(table), nil, "dept_roster")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	membersOf := func(dept string) []string {
		var raw gosql.NullString
		err := db.DB.QueryRow("SELECT members FROM "+mysqlBacktick(table)+" WHERE dept = ?", dept).Scan(&raw)
		if err == gosql.ErrNoRows || !raw.Valid {
			return nil
		}
		require.NoError(t, err)
		var arr []map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw.String), &arr))
		out := make([]string, len(arr))
		for i, m := range arr {
			out[i] = m["emp"].(string)
		}
		return out
	}
	apply := func(as ...*cluster.Actual) {
		for _, a := range as {
			_, err := projection.Sync(ctx, a)
			require.NoError(t, err)
		}
	}
	emp := func(id, dept string) *cluster.Actual {
		return sourceEvent(t, employeeType, id, map[string]any{"dept": dept, "emp": id})
	}

	// Two employees under dept A, one under dept B.
	apply(emp("e1", "A"), emp("e2", "A"), emp("e3", "B"))
	require.Equal(t, []string{"e1", "e2"}, membersOf("A"))
	require.Equal(t, []string{"e3"}, membersOf("B"))

	// Move e1 from A to B: it must leave A and join B, not appear in both.
	apply(emp("e1", "B"))
	require.Equal(t, []string{"e2"}, membersOf("A"), "the moved employee is gone from the old department")
	require.Equal(t, []string{"e1", "e3"}, membersOf("B"), "the moved employee joins the new department")
}

// mysqlMovieCardEnrichedConfig is movieCardEnrichedConfig with a JSON top_cast.
func mysqlMovieCardEnrichedConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: "tconst",
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "primary_title", SQLType: "VARCHAR(255)"},
			{Name: "top_cast", SQLType: "JSON"},
		},
		Sources: []sql.ProjectionSource{
			{
				Topic:    "title",
				OnDelete: "delete-row",
				Rules: []sql.ProjectionRule{{
					Set: []sql.ProjectionSet{{Column: "primary_title", From: "$.primary_title"}},
				}},
			},
			{
				Topic:  "name",
				Lookup: &sql.ProjectionLookup{Name: "names", Fields: []sql.ProjectionElementField{{Field: "primary_name", From: "$.primary_name"}}},
			},
			{
				Topic: "principal",
				Aggregate: &sql.ProjectionAggregate{
					Column:         "top_cast",
					ElementKey:     "$.ordering",
					ElementKeyType: "number",
					Element: []sql.ProjectionElementField{
						{Field: "nconst", From: "$.nconst"},
						{Field: "ordering", From: "$.ordering"},
						{Field: "name", Lookup: "names", On: "nconst", Select: "primary_name"},
					},
				},
			},
		},
	}
}

// TestMySQLIntegration_LookupEnrichment mirrors the Postgres enrichment
// criterion on real MySQL: aggregate elements resolve a foreign key to a
// dimension field by join, late-arriving dimensions fan out, a dimension
// change re-materializes dependents, a dimension delete nulls the enriched
// field but keeps the element, and rebuild-from-0 reproduces it.
func TestMySQLIntegration_LookupEnrichment(t *testing.T) {
	const table = "enr_movie_card_my"
	dropTableMySQLByName(t, table)
	defer dropTableMySQLByName(t, table)
	defer dropTableMySQLByName(t, table+"__top_cast")
	defer dropTableMySQLByName(t, table+"__lookup_names")

	db, err := sql.NewDB(&dialects.MySQLDialect{}, mysqlConn(t))
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, mysqlMovieCardEnrichedConfig(table), nil, "movie_card")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	principal := func(ordering int, nconst string) *cluster.Actual {
		return sourceEvent(t, principalType, fmt.Sprintf("[\"tt1\",\"%d\"]", ordering),
			map[string]any{"tconst": "tt1", "ordering": ordering, "nconst": nconst})
	}
	name := func(nconst, primaryName string) *cluster.Actual {
		return sourceEvent(t, nameType, nconst, map[string]any{"nconst": nconst, "primary_name": primaryName})
	}
	apply := func(as ...*cluster.Actual) {
		for _, a := range as {
			_, err := projection.Sync(ctx, a)
			require.NoError(t, err)
		}
	}

	// names map nconst → resolved name in top_cast (nil = present element, null
	// name; absent key = no such cast member).
	names := func() map[string]any {
		var raw gosql.NullString
		err := db.DB.QueryRow("SELECT top_cast FROM "+mysqlBacktick(table)+" WHERE tconst = ?", "tt1").Scan(&raw)
		if err == gosql.ErrNoRows || !raw.Valid {
			return map[string]any{}
		}
		require.NoError(t, err)
		var arr []map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw.String), &arr))
		out := map[string]any{}
		for _, m := range arr {
			out[m["nconst"].(string)] = m["name"]
		}
		return out
	}

	// Dimensions before facts: the names resolve on the principals' first fold.
	apply(
		sourceEvent(t, titleType, "tt1", map[string]any{"tconst": "tt1", "primary_title": "Heat"}),
		name("nm1", "Al Pacino"),
		name("nm2", "Robert De Niro"),
		principal(1, "nm1"),
		principal(2, "nm2"),
	)
	require.Equal(t, map[string]any{"nm1": "Al Pacino", "nm2": "Robert De Niro"}, names(),
		"the foreign key resolves to the dimension name")

	// Late-arriving dimension: a principal whose name does not exist yet folds
	// with a null name, then the name's arrival fans out and fills it in.
	apply(principal(3, "nm3"))
	require.Nil(t, names()["nm3"], "the cast member is present but its name is null until the dimension arrives")
	apply(name("nm3", "Val Kilmer"))
	require.Equal(t, "Val Kilmer", names()["nm3"], "the late dimension row fans out and fills the element in")

	// Dimension update: changing a name updates every element that references it.
	apply(name("nm1", "Alfredo James Pacino"))
	require.Equal(t, "Alfredo James Pacino", names()["nm1"], "a dimension change re-materializes its dependents")

	// Dimension delete: the enriched field nulls (LEFT JOIN), the element stays.
	apply(sourceDelete(nameType, "nm2"))
	got := names()
	require.Contains(t, got, "nm2", "the cast member survives a dimension delete")
	require.Nil(t, got["nm2"], "the enriched name nulls out")

	// Rebuild from 0 reproduces the enriched, fanned-out state.
	want := names()
	full := []*cluster.Actual{
		sourceEvent(t, titleType, "tt1", map[string]any{"tconst": "tt1", "primary_title": "Heat"}),
		name("nm1", "Al Pacino"), name("nm2", "Robert De Niro"),
		principal(1, "nm1"), principal(2, "nm2"),
		principal(3, "nm3"), name("nm3", "Val Kilmer"),
		name("nm1", "Alfredo James Pacino"), sourceDelete(nameType, "nm2"),
	}
	require.NoError(t, projection.Teardown())
	require.NoError(t, projection.Init())
	apply(full...)
	require.Equal(t, want, names(), "rebuild-from-0 reproduces the enriched top_cast")
}
