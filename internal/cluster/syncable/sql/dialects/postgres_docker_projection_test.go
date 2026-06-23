//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
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
