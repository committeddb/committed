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

var projectionEventType = &cluster.Type{ID: "controlplane-event", Name: "ControlplaneEvent"}

func projectionConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      table,
		PrimaryKey: "tenant_id",
		Columns: []sql.ProjectionColumn{
			{Name: "tenant_id", SQLType: "VARCHAR(64)"},
			{Name: "tier", SQLType: "VARCHAR(32)"},
			{Name: "state", SQLType: "VARCHAR(32)"},
			{Name: "allocs", SQLType: "JSONB"},
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

func projectionEvent(t *testing.T, key string, fields map[string]any) *cluster.Actual {
	t.Helper()
	bs, err := json.Marshal(fields)
	require.NoError(t, err)
	return &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(projectionEventType, []byte(key), bs),
	}}
}

// TestPostgreSQLIntegration_ProjectionLifecycle is the ticket's success
// criterion on real Postgres: the tenant lifecycle folds to one
// converged row per tenant (ON CONFLICT upserts restricted to each
// rule's columns), replaying everything twice over reproduces identical
// state, unmatched events leave no row, and delete Actuals hard-delete.
func TestPostgreSQLIntegration_ProjectionLifecycle(t *testing.T) {
	table := uniqueTable(t)
	defer dropTable(t, table)

	db, err := sql.NewDB(&dialects.PostgreSQLDialect{}, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, projectionConfig(table), nil, "tenants")
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
		rows, err := db.DB.Query("SELECT tenant_id, tier, state, allocs FROM " + table)
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

var (
	titleType  = &cluster.Type{ID: "title", Name: "Title"}
	ratingType = &cluster.Type{ID: "rating", Name: "Rating"}
)

func sourceEvent(t *testing.T, typ *cluster.Type, key string, fields map[string]any) *cluster.Actual {
	t.Helper()
	bs, err := json.Marshal(fields)
	require.NoError(t, err)
	return &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(typ, []byte(key), bs)}}
}

func sourceDelete(typ *cluster.Type, key string) *cluster.Actual {
	return &cluster.Actual{Entities: []*cluster.Entity{cluster.NewDeleteEntity(typ, []byte(key))}}
}

// movieCardConfig folds two normalized source topics — title (the spine) and
// rating (a contributor) — into one denormalized movie_card row keyed by
// tconst. Each source's rule matches all of its events (no when: the topic is
// the discriminator) and sets only its own columns, so the two fold without
// clobbering. The rating source clears its columns on delete (the movie
// survives); the title source drops the row.
func movieCardConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: "tconst",
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "primary_title", SQLType: "VARCHAR(255)"},
			{Name: "start_year", SQLType: "NUMERIC"},
			{Name: "genres", SQLType: "VARCHAR(255)"},
			{Name: "average_rating", SQLType: "NUMERIC"},
			{Name: "num_votes", SQLType: "NUMERIC"},
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

// TestPostgreSQLIntegration_MultiSourceProjection is the multisource success
// criterion on real Postgres: two source topics fold into one row with distinct
// columns (no clobber), a contributor's delete clears just its columns (the row
// survives), the spine's delete drops the row, and a replay-from-0 reproduces
// the fold.
func TestPostgreSQLIntegration_MultiSourceProjection(t *testing.T) {
	table := uniqueTable(t)
	defer dropTable(t, table)

	db, err := sql.NewDB(&dialects.PostgreSQLDialect{}, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, movieCardConfig(table), nil, "movie_card")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	type row struct{ Title, Year, Genres, Rating, Votes gosql.NullString }
	readRow := func(tconst string) (row, bool) {
		var r row
		err := db.DB.QueryRow(
			"SELECT primary_title, start_year, genres, average_rating, num_votes FROM "+table+" WHERE tconst = $1", tconst,
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
	require.Equal(t, "9.3", r.Rating.String, "rating folded into the same row")
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

var principalType = &cluster.Type{ID: "principal", Name: "Principal"}

// movieCardSplitConfig folds three sources into one movie_card row: title (the
// spine) plus the principal topic split by category into two array columns —
// actors into top_cast, directors into directors — ordered numerically by
// ordering. This is the filtered-aggregate shape: two aggregate sources share
// one topic.
func movieCardSplitConfig(table string) *sql.ProjectionConfig {
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
			{Name: "top_cast", SQLType: "JSONB"},
			{Name: "directors", SQLType: "JSONB"},
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

// TestPostgreSQLIntegration_AggregateProjection is the projection-aggregate
// success criterion on real Postgres: the principal topic splits by category
// into two array columns; the arrays order numerically by elementKey (10 after
// 3, not lexically before it); a re-delivered child replaces its element; a
// child delete removes exactly its element from the right column (the split
// self-selects) and leaves the row and the other column; and a teardown +
// rebuild-from-0 reproduces the same arrays.
func TestPostgreSQLIntegration_AggregateProjection(t *testing.T) {
	// Short fixed name: the sidecar (<table>__top_cast) and its index
	// (..._parent) append suffixes, and Postgres truncates identifiers at 63.
	const table = "agg_movie_card"
	dropTable(t, table)
	defer dropTable(t, table)
	defer dropTable(t, table+"__top_cast")
	defer dropTable(t, table+"__directors")

	db, err := sql.NewDB(&dialects.PostgreSQLDialect{}, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, movieCardSplitConfig(table), nil, "movie_card")
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
		err := db.DB.QueryRow("SELECT "+col+" FROM "+table+" WHERE tconst = $1", "tt1").Scan(&raw)
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
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE tconst = $1", "tt1").Scan(&n))
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
		require.NoError(t, db.DB.QueryRow("SELECT primary_title FROM "+table+" WHERE tconst=$1", "tt1").Scan(&s))
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
