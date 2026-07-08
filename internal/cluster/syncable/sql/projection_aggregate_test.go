package sql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

var principalType = &cluster.Type{ID: "principal", Name: "Principal"}

// topCastConfig folds the principal topic into a movie_card.top_cast array,
// keyed by tconst, ordered numerically by ordering.
func topCastConfig() *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      "movie_card",
		PrimaryKey: "tconst",
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "top_cast", SQLType: "JSONB"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:    "principal",
			KeyPath:  "$.tconst",
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
		PrimaryKey: "tconst",
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
		PrimaryKey: sql.SidecarChildKey,
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
		PrimaryKey: "tconst",
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
		PrimaryKey: "tconst",
		Mappings:   []sql.Mapping{{Column: "tconst", SQLType: "VARCHAR(16)"}, {Column: "top_cast", SQLType: "JSONB"}},
	}
	dimSpec := sql.LookupSpec{Dimension: "movie_card__lookup_names"}
	dimConfig := &sql.Config{
		Table:      "movie_card__lookup_names",
		PrimaryKey: sql.LookupKey,
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
		Table: "movie_card__top_cast", PrimaryKey: sql.SidecarChildKey,
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
