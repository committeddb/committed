package sql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

var principalType = &cluster.Type{ID: "principal", Name: "Principal"}

// topCastConfig folds the principal topic into a movie_card.top_cast array,
// keyed by tconst, ordered numerically by ordering.
func topCastConfig() *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      "movie_card",
		PrimaryKey: []string{"tconst"},
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "top_cast", SQLType: "JSONB"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:    "principal",
			KeyPath:  []string{"$.tconst"},
			OnDelete: "remove-from-aggregate",
			Aggregate: &sql.ProjectionAggregate{
				Column:         "top_cast",
				ElementKey:     "$.ordering",
				ElementKeyType: "number",
				Element:        []sql.ProjectionElementField{{Field: "nconst", From: "$.nconst"}},
			},
		}},
	}
}

// aggregatePrepares holds the five prepared-statement handles an aggregate
// source registers, so the test can attach exec/query expectations to each.
type aggregatePrepares struct {
	upsertSidecar *sqlmock.ExpectedPrepare
	deleteSidecar *sqlmock.ExpectedPrepare
	lookup        *sqlmock.ExpectedPrepare
	materialize   *sqlmock.ExpectedPrepare
	rebuild       *sqlmock.ExpectedPrepare
}

// newMockAggregateProjection wires a single-aggregate-source Projection against
// sqlmock, registering the Init expectations in the exact order Init issues
// them (main DDL, sidecar DDL, the five aggregate prepares, the shared
// row-delete prepare) and returning the prepare handles. Expected SQL is
// computed through the same dialect, so the strings match byte-for-byte.
func newMockAggregateProjection(t *testing.T) (*sql.Projection, sqlmock.Sqlmock, aggregatePrepares) {
	t.Helper()
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ddlConfig := &sql.Config{
		Table:      "movie_card",
		PrimaryKey: []string{"tconst"},
		Mappings: []sql.Mapping{
			{Column: "tconst", SQLType: "VARCHAR(16)"},
			{Column: "top_cast", SQLType: "JSONB"},
		},
	}
	spec := sql.AggregateSpec{
		Table:       "movie_card",
		PrimaryKey:  "tconst",
		Column:      "top_cast",
		Sidecar:     "movie_card__top_cast",
		NumericSort: true,
	}
	scConfig := &sql.Config{
		Table:      "movie_card__top_cast",
		PrimaryKey: []string{sql.SidecarChildKey},
		Mappings: []sql.Mapping{
			{Column: sql.SidecarChildKey},
			{Column: sql.SidecarParentKey},
			{Column: sql.SidecarElementKey},
			{Column: sql.SidecarElement},
		},
	}

	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec(dialect.CreateAggregateSidecarDDL(spec)).WillReturnResult(driver.ResultNoRows)
	p := aggregatePrepares{
		upsertSidecar: mock.ExpectPrepare(dialect.CreateSQL(scConfig)),
		deleteSidecar: mock.ExpectPrepare(dialect.CreateDeleteSQL(scConfig)),
		lookup:        mock.ExpectPrepare(dialect.CreateAggregateParentLookupSQL(spec)),
		materialize:   mock.ExpectPrepare(dialect.CreateAggregateMaterializeSQL(spec)),
		rebuild:       mock.ExpectPrepare(dialect.CreateAggregateRebuildSQL(spec)),
	}
	mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	projection := sql.NewProjection(db, topCastConfig(), nil, "movie_card")
	require.NoError(t, projection.Init())
	return projection, mock, p
}

// An upsert records the child in the sidecar (key-first, MySQL-doubled args)
// then re-materializes the parent column; both materialize placeholders bind
// the parent key.
func TestProjectionAggregateUpsert(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	// child_key, parent_key, element_key (numeric key stored as text), element.
	sidecarArgs := []driver.Value{`["tt1","1"]`, "tt1", "1", `{"nconst":"nm1"}`}
	sidecarArgs = append(sidecarArgs, sidecarArgs...) // mock dialect doubles like MySQL

	mock.ExpectBegin()
	// The prior-parent lookup runs first; a new child has no sidecar row, so no
	// old parent to rebuild.
	p.lookup.ExpectQuery().WithArgs(`["tt1","1"]`).
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}))
	p.upsertSidecar.ExpectExec().WithArgs(sidecarArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.materialize.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		principalType, []byte(`["tt1","1"]`),
		[]byte(`{"tconst":"tt1","ordering":1,"nconst":"nm1","category":"actor"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestProjectionAggregateReparenting is the stale-element regression: a child
// re-delivered under a different parent must have its OLD parent rebuilt too, or
// that parent's array keeps an element the child no longer belongs to. The
// sidecar records the new parent; the fix additionally rebuilds the old one.
func TestProjectionAggregateReparenting(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	const childKey = "nm1"
	sidecarArgs := []driver.Value{childKey, "tt2", "1", `{"nconst":"nm1"}`}
	sidecarArgs = append(sidecarArgs, sidecarArgs...) // mock dialect doubles like MySQL

	mock.ExpectBegin()
	// The child currently sits under tt1; this event moves it to tt2.
	p.lookup.ExpectQuery().WithArgs(childKey).
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}).AddRow("tt1"))
	p.upsertSidecar.ExpectExec().WithArgs(sidecarArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.materialize.ExpectExec().WithArgs("tt2", "tt2").WillReturnResult(sqlmock.NewResult(0, 1))
	// The fix: the old parent tt1 is rebuilt so it drops the moved element.
	p.rebuild.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		principalType, []byte(childKey),
		[]byte(`{"tconst":"tt2","ordering":1,"nconst":"nm1","category":"actor"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestProjectionAggregateInPlaceUpdateNoRebuild pins the guard: a child
// re-delivered under the SAME parent must not trigger a redundant old-parent
// rebuild — only the sidecar upsert and the single materialize run.
func TestProjectionAggregateInPlaceUpdateNoRebuild(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	const childKey = "nm1"
	sidecarArgs := []driver.Value{childKey, "tt1", "1", `{"nconst":"nm1"}`}
	sidecarArgs = append(sidecarArgs, sidecarArgs...) // mock dialect doubles like MySQL

	mock.ExpectBegin()
	// The child already sits under tt1 and stays there — no rebuild expected.
	p.lookup.ExpectQuery().WithArgs(childKey).
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}).AddRow("tt1"))
	p.upsertSidecar.ExpectExec().WithArgs(sidecarArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.materialize.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		principalType, []byte(childKey),
		[]byte(`{"tconst":"tt1","ordering":1,"nconst":"nm1","category":"actor"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A delete recovers the parent from the sidecar, removes the sidecar row, then
// rebuilds the parent column from what remains.
func TestProjectionAggregateDelete(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	mock.ExpectBegin()
	p.lookup.ExpectQuery().WithArgs(`["tt1","1"]`).
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}).AddRow("tt1"))
	p.deleteSidecar.ExpectExec().WithArgs(`["tt1","1"]`).WillReturnResult(sqlmock.NewResult(0, 1))
	p.rebuild.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(principalType, []byte(`["tt1","1"]`)),
	}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A delete for a child this source never folded (sidecar lookup finds no row)
// is a no-op — no sidecar delete, no rebuild. This is what makes a split by
// when self-select: the delete routes to every source on the topic, but only
// the one holding the child shrinks.
func TestProjectionAggregateDeleteUnknownChildIsNoOp(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	mock.ExpectBegin()
	p.lookup.ExpectQuery().WithArgs(`["tt9","9"]`).
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"})) // empty
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(principalType, []byte(`["tt9","9"]`)),
	}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

var nameType = &cluster.Type{ID: "name", Name: "Name"}

// enrichedConfig folds a names lookup (dimension) and a principal aggregate whose
// cast element enriches name from that dimension by nconst.
func enrichedConfig() *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      "movie_card",
		PrimaryKey: []string{"tconst"},
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "top_cast", SQLType: "JSONB"},
		},
		Sources: []sql.ProjectionSource{
			{
				Topic:  "name",
				Lookup: &sql.ProjectionLookup{Name: "names", Fields: []sql.ProjectionElementField{{Field: "primary_name", From: "$.primary_name"}}},
			},
			{
				Topic: "principal",
				Aggregate: &sql.ProjectionAggregate{
					Column:         "top_cast",
					ElementKey:     "$.ordering",
					ElementKeyType: "text",
					Element: []sql.ProjectionElementField{
						{Field: "nconst", From: "$.nconst"},
						{Field: "name", Lookup: "names", On: "nconst", Select: "primary_name"},
					},
				},
			},
		},
	}
}

type enrichedPrepares struct {
	dimUpsert *sqlmock.ExpectedPrepare
	dimDelete *sqlmock.ExpectedPrepare
	affected  *sqlmock.ExpectedPrepare
	rebuild   *sqlmock.ExpectedPrepare
}

// newMockEnrichedProjection registers the Init expectations for enrichedConfig in
// the exact order Init issues them (main DDL; the lookup's dimension DDL +
// upsert/delete prepares; the aggregate's sidecar DDL + five prepares; the
// fan-out affected-parents prepare; the shared row-delete prepare) and returns
// the handles the fan-out tests attach to.
func newMockEnrichedProjection(t *testing.T) (*sql.Projection, sqlmock.Sqlmock, enrichedPrepares) {
	t.Helper()
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ddlConfig := &sql.Config{
		Table:      "movie_card",
		PrimaryKey: []string{"tconst"},
		Mappings:   []sql.Mapping{{Column: "tconst", SQLType: "VARCHAR(16)"}, {Column: "top_cast", SQLType: "JSONB"}},
	}
	dimSpec := sql.LookupSpec{Dimension: "movie_card__lookup_names"}
	dimConfig := &sql.Config{
		Table:      "movie_card__lookup_names",
		PrimaryKey: []string{sql.LookupKey},
		Mappings:   []sql.Mapping{{Column: sql.LookupKey}, {Column: sql.LookupFields}},
	}
	aggSpec := sql.AggregateSpec{
		Table: "movie_card", PrimaryKey: "tconst", Column: "top_cast",
		Sidecar: "movie_card__top_cast",
		Enrichments: []sql.AggregateEnrichment{{
			Dimension: "movie_card__lookup_names", OnField: "nconst",
			Selects: []sql.AggregateEnrichmentField{{Output: "name", Source: "primary_name"}},
		}},
	}
	scConfig := &sql.Config{
		Table: "movie_card__top_cast", PrimaryKey: []string{sql.SidecarChildKey},
		Mappings: []sql.Mapping{{Column: sql.SidecarChildKey}, {Column: sql.SidecarParentKey}, {Column: sql.SidecarElementKey}, {Column: sql.SidecarElement}},
	}

	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	// lookup source
	mock.ExpectExec(dialect.CreateLookupDimensionDDL(dimSpec)).WillReturnResult(driver.ResultNoRows)
	p := enrichedPrepares{dimUpsert: mock.ExpectPrepare(dialect.CreateSQL(dimConfig))}
	p.dimDelete = mock.ExpectPrepare(dialect.CreateDeleteSQL(dimConfig))
	// aggregate source
	mock.ExpectExec(dialect.CreateAggregateSidecarDDL(aggSpec)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectPrepare(dialect.CreateSQL(scConfig))
	mock.ExpectPrepare(dialect.CreateDeleteSQL(scConfig))
	mock.ExpectPrepare(dialect.CreateAggregateParentLookupSQL(aggSpec))
	mock.ExpectPrepare(dialect.CreateAggregateMaterializeSQL(aggSpec))
	p.rebuild = mock.ExpectPrepare(dialect.CreateAggregateRebuildSQL(aggSpec))
	// fan-out wiring + shared row-delete
	p.affected = mock.ExpectPrepare(dialect.CreateAggregateAffectedParentsSQL(aggSpec, "nconst"))
	mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	projection := sql.NewProjection(db, enrichedConfig(), nil, "movie_card")
	require.NoError(t, projection.Init())
	return projection, mock, p
}

// A dimension upsert stores the row, then fans out: it finds the parents whose
// elements reference the changed key and rebuilds each.
func TestProjectionLookupUpsertFansOut(t *testing.T) {
	projection, mock, p := newMockEnrichedProjection(t)

	dimArgs := []driver.Value{"nm1", `{"primary_name":"Al Pacino"}`}
	dimArgs = append(dimArgs, dimArgs...) // mock dialect doubles like MySQL

	mock.ExpectBegin()
	p.dimUpsert.ExpectExec().WithArgs(dimArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.affected.ExpectQuery().WithArgs("nm1").
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}).AddRow("tt1").AddRow("tt2"))
	p.rebuild.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	p.rebuild.ExpectExec().WithArgs("tt2", "tt2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		nameType, []byte("nm1"), []byte(`{"nconst":"nm1","primary_name":"Al Pacino"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A dimension delete drops the row, then fans out the same way (dependents
// rebuild with the enriched field now null).
func TestProjectionLookupDeleteFansOut(t *testing.T) {
	projection, mock, p := newMockEnrichedProjection(t)

	mock.ExpectBegin()
	p.dimDelete.ExpectExec().WithArgs("nm1").WillReturnResult(sqlmock.NewResult(0, 1))
	p.affected.ExpectQuery().WithArgs("nm1").
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}).AddRow("tt1"))
	p.rebuild.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewDeleteEntity(nameType, []byte("nm1"))}}
	_, err := projection.Sync(context.Background(), actual)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestProjectionAggregateLookupErrorRedacted is the PII-egress regression for the
// aggregate prior-parent lookup: the query binds the child's entity Key (an RTBF
// subject), and a driver error can echo the bound value. The dead-letter + stuck-
// status egress redacts only cluster.RedactedError-typed errors, so this path's
// error MUST satisfy it — otherwise the raw driver text (with the subject key)
// lands in the permanent replicated dead-letter and the GET status/errors
// responses.
func TestProjectionAggregateLookupErrorRedacted(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	const childKey = "pii-subject@example.com" // the entity Key bound into the lookup
	driverErr := errors.New("pq: could not serialize access; Key (child_key)=(" + childKey + ") conflicts")

	mock.ExpectBegin()
	p.lookup.ExpectQuery().WithArgs(childKey).WillReturnError(driverErr)
	mock.ExpectRollback()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		principalType, []byte(childKey),
		[]byte(`{"tconst":"tt1","ordering":1,"nconst":"nm1","category":"actor"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	var red cluster.RedactedError
	require.True(t, errors.As(err, &red), "aggregate prior-parent lookup error must be a RedactedError")
	require.NotContains(t, red.RedactedMessage(), childKey, "redacted message must not echo the bound key")
	require.Contains(t, err.Error(), childKey, "node-local Error() keeps the full driver detail")
}

// TestProjectionFanOutErrorRedacted is the same regression for the lookup/fan-out
// path: the affected-parents query binds the changed dimension key, and a driver
// error can echo it. The error must be a RedactedError so the egress redacts it.
func TestProjectionFanOutErrorRedacted(t *testing.T) {
	projection, mock, p := newMockEnrichedProjection(t)

	const dimKey = "nm-pii-subject@example.com" // the dimension entity Key bound into fan-out
	dimArgs := []driver.Value{dimKey, `{"primary_name":"Al Pacino"}`}
	dimArgs = append(dimArgs, dimArgs...) // mock dialect doubles like MySQL
	driverErr := errors.New("pq: deadlock detected; Key (nconst)=(" + dimKey + ") referenced")

	mock.ExpectBegin()
	p.dimUpsert.ExpectExec().WithArgs(dimArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.affected.ExpectQuery().WithArgs(dimKey).WillReturnError(driverErr)
	mock.ExpectRollback()

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		nameType, []byte(dimKey), []byte(`{"nconst":"`+dimKey+`","primary_name":"Al Pacino"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	var red cluster.RedactedError
	require.True(t, errors.As(err, &red), "fan-out affected-parents query error must be a RedactedError")
	require.NotContains(t, red.RedactedMessage(), dimKey, "redacted message must not echo the bound key")
	require.Contains(t, err.Error(), dimKey)
}

// TestProjectionCommitErrorRedacted covers the commit boundary: a deferred-
// constraint violation surfaces at tx.Commit() (past the per-exec RedactedError
// coverage) and pgx/mysql can echo Key (col)=(value). The commit error must also
// be redacted before it reaches the replicated dead-letter / stuck status.
func TestProjectionCommitErrorRedacted(t *testing.T) {
	projection, mock, p := newMockAggregateProjection(t)

	const childKey = "pii-subject@example.com"
	sidecarArgs := []driver.Value{childKey, "tt1", "1", `{"nconst":"nm1"}`}
	sidecarArgs = append(sidecarArgs, sidecarArgs...) // mock dialect doubles like MySQL
	commitErr := errors.New("pq: deferred constraint violated; Key (parent_key)=(" + childKey + ")")

	mock.ExpectBegin()
	p.lookup.ExpectQuery().WithArgs(childKey).WillReturnRows(sqlmock.NewRows([]string{"parent_key"}))
	p.upsertSidecar.ExpectExec().WithArgs(sidecarArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.materialize.ExpectExec().WithArgs("tt1", "tt1").WillReturnResult(sqlmock.NewResult(0, 1))
	// A failed Commit finalizes the tx itself; the code does not (and must not)
	// Rollback after it, so no ExpectRollback here.
	mock.ExpectCommit().WillReturnError(commitErr)

	actual := &cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(
		principalType, []byte(childKey),
		[]byte(`{"tconst":"tt1","ordering":1,"nconst":"nm1","category":"actor"}`),
	)}}
	_, err := projection.Sync(context.Background(), actual)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	var red cluster.RedactedError
	require.True(t, errors.As(err, &red), "commit error must be a RedactedError")
	require.NotContains(t, red.RedactedMessage(), childKey, "redacted message must not echo the bound key")
	require.Contains(t, err.Error(), childKey)
}

// Scalar aggregate columns ride the same materialize/rebuild statements as
// the array column, with one extra parent-key bind per scalar. This pins the
// spec-driven bind plumbing end to end: a child upsert re-materializes with
// parentBinds repeats, and a child delete rebuilds with the same count.
func TestProjectionAggregateScalars(t *testing.T) {
	config := &sql.ProjectionConfig{
		Table:      "jobs",
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(16)"},
			{Name: "visits", SQLType: "JSONB"},
			{Name: "visit_count", SQLType: "INT"},
			{Name: "hours_sum", SQLType: "DECIMAL(12,2)"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:    "visit",
			KeyPath:  []string{"$.job_id"},
			OnDelete: "remove-from-aggregate",
			Aggregate: &sql.ProjectionAggregate{
				Column:     "visits",
				ElementKey: "$.id",
				Element:    []sql.ProjectionElementField{{Field: "hours", From: "$.hours"}},
				Scalars: []sql.ProjectionScalar{
					{Column: "visit_count", Fn: "count"},
					{Column: "hours_sum", Fn: "sum", Of: "hours"},
				},
			},
		}},
	}

	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ddlConfig := &sql.Config{
		Table:      "jobs",
		PrimaryKey: []string{"job_id"},
		Mappings: []sql.Mapping{
			{Column: "job_id", SQLType: "VARCHAR(16)"},
			{Column: "visits", SQLType: "JSONB"},
			{Column: "visit_count", SQLType: "INT"},
			{Column: "hours_sum", SQLType: "DECIMAL(12,2)"},
		},
	}
	spec := sql.AggregateSpec{
		Table: "jobs", PrimaryKey: "job_id", Column: "visits", Sidecar: "jobs__visits",
		Scalars: []sql.AggregateScalar{
			{Column: "visit_count", Fn: "count"},
			{Column: "hours_sum", Fn: "sum", Of: "hours"},
		},
	}
	scConfig := &sql.Config{
		Table:      "jobs__visits",
		PrimaryKey: []string{sql.SidecarChildKey},
		Mappings: []sql.Mapping{
			{Column: sql.SidecarChildKey},
			{Column: sql.SidecarParentKey},
			{Column: sql.SidecarElementKey},
			{Column: sql.SidecarElement},
		},
	}

	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec(dialect.CreateAggregateSidecarDDL(spec)).WillReturnResult(driver.ResultNoRows)
	p := aggregatePrepares{
		upsertSidecar: mock.ExpectPrepare(dialect.CreateSQL(scConfig)),
		deleteSidecar: mock.ExpectPrepare(dialect.CreateDeleteSQL(scConfig)),
		lookup:        mock.ExpectPrepare(dialect.CreateAggregateParentLookupSQL(spec)),
		materialize:   mock.ExpectPrepare(dialect.CreateAggregateMaterializeSQL(spec)),
		rebuild:       mock.ExpectPrepare(dialect.CreateAggregateRebuildSQL(spec)),
	}
	mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	projection := sql.NewProjection(db, config, nil, "jobs")
	require.NoError(t, projection.Init())

	// Upsert: array + 2 scalars + inserted key = 4 parent-key binds.
	sidecarArgs := []driver.Value{"v1", "j1", "v1", `{"hours":2.5}`}
	sidecarArgs = append(sidecarArgs, sidecarArgs...)
	mock.ExpectBegin()
	p.lookup.ExpectQuery().WithArgs("v1").
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}))
	p.upsertSidecar.ExpectExec().WithArgs(sidecarArgs...).WillReturnResult(sqlmock.NewResult(0, 1))
	p.materialize.ExpectExec().WithArgs("j1", "j1", "j1", "j1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	visitType := &cluster.Type{ID: "visit", Name: "Visit"}
	_, err = projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(visitType, []byte("v1"), []byte(`{"job_id":"j1","id":"v1","hours":2.5}`)),
	}})
	require.NoError(t, err)

	// Delete: rebuild carries the same 4 parent-key binds.
	mock.ExpectBegin()
	p.lookup.ExpectQuery().WithArgs("v1").
		WillReturnRows(sqlmock.NewRows([]string{"parent_key"}).AddRow("j1"))
	p.deleteSidecar.ExpectExec().WithArgs("v1").WillReturnResult(sqlmock.NewResult(0, 1))
	p.rebuild.ExpectExec().WithArgs("j1", "j1", "j1", "j1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	_, err = projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(visitType, []byte("v1")),
	}})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The forEach engine end to end against sqlmock: one event fans N rows (one
// per element, keyed by the element, with $parent reaching the event), a
// re-emitted parent reconciles (the vanished element's row deletes), and a
// parent tombstone cascades to every fanned row via the sidecar.
func TestProjectionForEach(t *testing.T) {
	txnType := &cluster.Type{ID: "txn", Name: "Txn"}
	config := &sql.ProjectionConfig{
		Table:      "txn_elements",
		PrimaryKey: []string{"element_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "element_id", SQLType: "VARCHAR(64)"},
			{Name: "amount", SQLType: "DECIMAL(12,2)"},
			{Name: "txn_id", SQLType: "VARCHAR(64)"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:   "txn",
			KeyPath: []string{"$.sku"},
			ForEach: "$.items[*]",
			Rules: []sql.ProjectionRule{{
				Set: []sql.ProjectionSet{
					{Column: "amount", From: "$.amount"},
					{Column: "txn_id", From: "$parent.id"},
				},
			}},
		}},
	}

	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ddlConfig := &sql.Config{
		Table:      "txn_elements",
		PrimaryKey: []string{"element_id"},
		Mappings: []sql.Mapping{
			{Column: "element_id", SQLType: "VARCHAR(64)"},
			{Column: "amount", SQLType: "DECIMAL(12,2)"},
			{Column: "txn_id", SQLType: "VARCHAR(64)"},
		},
	}
	ruleConfig := &sql.Config{
		Table:      "txn_elements",
		PrimaryKey: []string{"element_id"},
		Mappings: []sql.Mapping{
			{Column: "element_id"}, {Column: "amount"}, {Column: "txn_id"},
		},
	}
	sidecar := sql.ForEachSidecarName("txn_elements", "txn")
	scConfig := &sql.Config{
		Table:      sidecar,
		PrimaryKey: []string{sql.SidecarChildKey},
		Mappings: []sql.Mapping{
			{Column: sql.SidecarChildKey},
			{Column: sql.SidecarParentKey},
			{Column: sql.SidecarElementKey},
			{Column: sql.SidecarElement},
		},
	}
	spec := sql.AggregateSpec{Table: "txn_elements", PrimaryKey: "element_id", Sidecar: sidecar}

	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	rulePrepare := mock.ExpectPrepare(dialect.CreateSQL(ruleConfig))
	mock.ExpectExec(dialect.CreateAggregateSidecarDDL(spec)).WillReturnResult(driver.ResultNoRows)
	scUpsert := mock.ExpectPrepare(dialect.CreateSQL(scConfig))
	scDelete := mock.ExpectPrepare(dialect.CreateDeleteSQL(scConfig))
	children := mock.ExpectPrepare(dialect.CreateForEachChildrenSQL(sidecar))
	rowDelete := mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	p := sql.NewProjection(db, config, nil, "txn_elements")
	require.NoError(t, p.Init())

	sync := func(a *cluster.Actual) {
		t.Helper()
		_, err := p.Sync(context.Background(), a)
		require.NoError(t, err)
	}
	scArgs := func(child, parent string) []driver.Value {
		args := []driver.Value{child, parent, "", "{}"}
		return append(args, args...) // mock dialect doubles like MySQL
	}
	rowArgs := func(vals ...driver.Value) []driver.Value { return append(vals, vals...) }

	// Fan-out: two elements → children query (no prior), two row upserts
	// (element-scoped amount, $parent-scoped txn id), two sidecar upserts.
	mock.ExpectBegin()
	children.ExpectQuery().WithArgs("p1").WillReturnRows(sqlmock.NewRows([]string{"child_key"}))
	rulePrepare.ExpectExec().WithArgs(rowArgs("a", "2.50", "p1")...).WillReturnResult(sqlmock.NewResult(0, 1))
	scUpsert.ExpectExec().WithArgs(scArgs("a", "p1")...).WillReturnResult(sqlmock.NewResult(0, 1))
	rulePrepare.ExpectExec().WithArgs(rowArgs("b", "1.25", "p1")...).WillReturnResult(sqlmock.NewResult(0, 1))
	scUpsert.ExpectExec().WithArgs(scArgs("b", "p1")...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(&cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(txnType, []byte("p1"),
		[]byte(`{"id":"p1","items":[{"sku":"a","amount":"2.50"},{"sku":"b","amount":"1.25"}]}`))}})

	// Reconcile: the re-emitted parent lost element b → its row and sidecar
	// entry delete; element a upserts as usual.
	mock.ExpectBegin()
	children.ExpectQuery().WithArgs("p1").
		WillReturnRows(sqlmock.NewRows([]string{"child_key"}).AddRow("a").AddRow("b"))
	rulePrepare.ExpectExec().WithArgs(rowArgs("a", "3.00", "p1")...).WillReturnResult(sqlmock.NewResult(0, 1))
	scUpsert.ExpectExec().WithArgs(scArgs("a", "p1")...).WillReturnResult(sqlmock.NewResult(0, 1))
	rowDelete.ExpectExec().WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	scDelete.ExpectExec().WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(&cluster.Actual{Entities: []*cluster.Entity{cluster.NewUpsertEntity(txnType, []byte("p1"),
		[]byte(`{"id":"p1","items":[{"sku":"a","amount":"3.00"}]}`))}})

	// Cascade: the parent tombstone deletes every fanned row via the sidecar.
	mock.ExpectBegin()
	children.ExpectQuery().WithArgs("p1").
		WillReturnRows(sqlmock.NewRows([]string{"child_key"}).AddRow("a"))
	rowDelete.ExpectExec().WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	scDelete.ExpectExec().WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(&cluster.Actual{Entities: []*cluster.Entity{cluster.NewDeleteEntity(txnType, []byte("p1"))}})

	require.NoError(t, mock.ExpectationsWereMet())
}

// Stages end to end through Sync: topic entities fold through a
// reshape→aggregate chain in the stage store, and the table source
// consuming the aggregate stage (from = "by-job") lands its deltas as
// rows — including the retraction when a key's last input vanishes.
func TestProjectionStagesToTable(t *testing.T) {
	txnType := &cluster.Type{ID: "txns", Name: "Txn"}
	config := &sql.ProjectionConfig{
		Table:      "job_totals",
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(64)"},
			{Name: "total", SQLType: "DECIMAL(12,2)"},
			{Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{
			{
				Name: "live", From: "txns", KeyPath: []string{"$.id"},
				Emit: []sql.StageEmit{{Field: "job", From: "$.jobId"}, {Field: "amt", From: "$.amount"}},
			},
			{
				Name: "by-job", From: "live", KeyPath: []string{"$.job"},
				Reduce: "aggregate", Emit: []sql.StageEmit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
			},
		},
		Sources: []sql.ProjectionSource{{
			FromStage: "by-job",
			KeyPath:   []string{"$.job"},
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
				{Column: "total", From: "$.total"},
				{Column: "n", From: "$.n"},
			}}},
		}},
	}

	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ddlConfig := &sql.Config{
		Table:      "job_totals",
		PrimaryKey: []string{"job_id"},
		Mappings: []sql.Mapping{
			{Column: "job_id", SQLType: "VARCHAR(64)"},
			{Column: "total", SQLType: "DECIMAL(12,2)"},
			{Column: "n", SQLType: "INT"},
		},
	}
	ruleConfig := &sql.Config{
		Table:      "job_totals",
		PrimaryKey: []string{"job_id"},
		Mappings:   []sql.Mapping{{Column: "job_id"}, {Column: "total"}, {Column: "n"}},
	}
	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	rulePrepare := mock.ExpectPrepare(dialect.CreateSQL(ruleConfig))
	rowDelete := mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	p := sql.NewProjection(db, config, nil, "job_totals")
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	t.Cleanup(func() { _ = p.Close() })

	sync := func(idx uint64, e *cluster.Entity) {
		t.Helper()
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{e}})
		require.NoError(t, err)
	}
	rowArgs := func(vals ...driver.Value) []driver.Value { return append(vals, vals...) }

	// First txn: j1 gains its first input → the by-job delta upserts a row.
	mock.ExpectBegin()
	rulePrepare.ExpectExec().WithArgs(rowArgs("j1", "2.5", int64(1))...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(10, cluster.NewUpsertEntity(txnType, []byte("t1"), []byte(`{"id":"t1","jobId":"j1","amount":2.5}`)))

	// The SAME entity redelivered: every refold lands on identical bytes,
	// so the cascade suppresses — one Begin/Commit, ZERO table writes
	// (determinism making redelivery free, not just safe).
	mock.ExpectBegin()
	mock.ExpectCommit()
	sync(10, cluster.NewUpsertEntity(txnType, []byte("t1"), []byte(`{"id":"t1","jobId":"j1","amount":2.5}`)))

	// Second txn, same job: the aggregate refolds — exact sum, count 2.
	mock.ExpectBegin()
	rulePrepare.ExpectExec().WithArgs(rowArgs("j1", "3.75", int64(2))...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(11, cluster.NewUpsertEntity(txnType, []byte("t2"), []byte(`{"id":"t2","jobId":"j1","amount":1.25}`)))

	// Deleting one txn refolds down; deleting the last RETRACTS the row.
	mock.ExpectBegin()
	rulePrepare.ExpectExec().WithArgs(rowArgs("j1", "2.5", int64(1))...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(12, cluster.NewDeleteEntity(txnType, []byte("t2")))

	mock.ExpectBegin()
	rowDelete.ExpectExec().WithArgs("j1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	sync(13, cluster.NewDeleteEntity(txnType, []byte("t1")))

	require.NoError(t, mock.ExpectationsWereMet())
}

// Teardown erases the stage store with the destination — left behind, a
// rebuild's replay-from-0 would refold onto identical bytes and SUPPRESS
// every table delta (an empty rebuilt table, silently). After teardown, a
// fresh Init opens a fresh store and the same event folds and LANDS again.
func TestProjectionTeardownResetsStageStore(t *testing.T) {
	txnType := &cluster.Type{ID: "txns", Name: "Txn"}
	newConfig := func() *sql.ProjectionConfig {
		return &sql.ProjectionConfig{
			Table:      "job_totals",
			PrimaryKey: []string{"job_id"},
			Columns: []sql.ProjectionColumn{
				{Name: "job_id", SQLType: "VARCHAR(64)"},
				{Name: "n", SQLType: "INT"},
			},
			Stages: []sql.ProjectionStage{
				{
					Name: "by-job", From: "txns", KeyPath: []string{"$.job"},
					Reduce: "aggregate", Emit: []sql.StageEmit{{Field: "n", Count: true}},
				},
			},
			Sources: []sql.ProjectionSource{{
				FromStage: "by-job",
				KeyPath:   []string{"$.job"},
				Rules:     []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "n", From: "$.n"}}}},
			}},
		}
	}
	ddlConfig := &sql.Config{
		Table:      "job_totals",
		PrimaryKey: []string{"job_id"},
		Mappings:   []sql.Mapping{{Column: "job_id", SQLType: "VARCHAR(64)"}, {Column: "n", SQLType: "INT"}},
	}
	ruleConfig := &sql.Config{
		Table:      "job_totals",
		PrimaryKey: []string{"job_id"},
		Mappings:   []sql.Mapping{{Column: "job_id"}, {Column: "n"}},
	}
	storeDir := t.TempDir()

	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	boot := func() *sql.Projection {
		mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
		rulePrepare := mock.ExpectPrepare(dialect.CreateSQL(ruleConfig))
		mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))
		p := sql.NewProjection(db, newConfig(), nil, "job_totals")
		p.SetStoreDir(storeDir)
		require.NoError(t, p.Init())
		mock.ExpectBegin()
		rulePrepare.ExpectExec().WithArgs("j1", int64(1), "j1", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: 10, Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(txnType, []byte("t1"), []byte(`{"job":"j1"}`)),
		}})
		require.NoError(t, err)
		return p
	}

	p := boot()

	// Teardown: sink drops AND the stage store file goes with them.
	mock.ExpectExec(dialect.DropDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	require.NoError(t, p.Teardown())
	_, statErr := os.Stat(stagestore.FilePath(storeDir, "job_totals"))
	require.True(t, os.IsNotExist(statErr), "teardown must remove the stage store")
	require.NoError(t, p.Close())

	// A fresh boot replays the SAME event: with the store reset it folds
	// and LANDS again — no suppression from stale state.
	p2 := boot()
	require.NoError(t, p2.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}
