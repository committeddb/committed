package sql_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"

	"github.com/DATA-DOG/go-sqlmock"
)

var (
	simpleType    = &cluster.Type{ID: "simple", Name: "simple"}
	notSimpleType = &cluster.Type{ID: "notSimple", Name: "notSimple"}
)

func TestSync(t *testing.T) {
	simpleOne := simpleEntity("key1", "one")
	simpleTwo := simpleEntity("key2", "two")
	simpleThree := simpleEntity("key3", "three")

	tests := []struct {
		name           string
		data           [][]*Entity
		configFileName string
	}{
		{"one-simple", [][]*Entity{{simpleOne}}, "./simple_syncable.toml"},
		{"two-simple", [][]*Entity{{simpleOne, simpleTwo}}, "./simple_syncable.toml"},
		{"two-one-simple", [][]*Entity{{simpleOne, simpleTwo}, {simpleThree}}, "./simple_syncable.toml"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect, mock, err := dialects.NewSQLMockDialect()
			require.Nil(t, err)

			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)
			v := readConfig(t, "toml", bytes.NewReader(bs))

			p := &sql.SyncableParser{}

			db, err := sql.NewDB(dialect, "")
			require.Nil(t, err)
			defer db.Close()

			dbs := make(map[string]cluster.Database)
			dbs["testdb"] = db
			config, err := p.ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
			require.Nil(t, err)

			ddlSQL := dialect.CreateDDL(config)
			mock.ExpectExec(ddlSQL).WillReturnResult(driver.ResultNoRows)
			insertSQL := dialect.CreateSQL(config)
			expectedPrepare := mock.ExpectPrepare(insertSQL)
			// Init also prepares the DELETE-by-key statement (the config
			// names a primaryKey, so deletes are honorable).
			mock.ExpectPrepare(dialect.CreateDeleteSQL(config))

			syncable := sql.New(db, config)
			err = syncable.Init()
			require.Nil(t, err)

			ctx := context.Background()

			total := int64(0)
			for oi, p := range createProposals(t, tt.data) {
				mock.ExpectBegin()
				for ii := range p.Entities {
					e := tt.data[oi][ii]
					total++
					dvs := getDriverValues(e.Args)
					result := sqlmock.NewResult(total, 1)
					allDVS := append(dvs, dvs...)
					expectedPrepare.ExpectExec().WithArgs(allDVS...).WillReturnResult(result)
				}
				mock.ExpectCommit()
				shouldSnapshot, err := syncable.Sync(ctx, p)
				require.Nil(t, err)
				require.Equal(t, cluster.ShouldSnapshot(true), shouldSnapshot)
			}

			require.Nil(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDontSyncOtherTypes(t *testing.T) {
	simpleOne := simpleEntity("key1", "one")
	notSimpleOne := notSimpleEntity("key1", "one")

	tests := []struct {
		name           string
		data           [][]*Entity
		configFileName string
	}{
		{"one-simple", [][]*Entity{{notSimpleOne}}, "./simple_syncable.toml"},
		{"two-simple", [][]*Entity{{simpleOne, notSimpleOne}}, "./simple_syncable.toml"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect, mock, err := dialects.NewSQLMockDialect()
			require.Nil(t, err)

			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)
			v := readConfig(t, "toml", bytes.NewReader(bs))

			p := &sql.SyncableParser{}

			db, err := sql.NewDB(dialect, "")
			require.Nil(t, err)
			defer db.Close()

			dbs := make(map[string]cluster.Database)
			dbs["testdb"] = db
			config, err := p.ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
			require.Nil(t, err)

			ddlSQL := dialect.CreateDDL(config)
			mock.ExpectExec(ddlSQL).WillReturnResult(driver.ResultNoRows)
			insertSQL := dialect.CreateSQL(config)
			mock.ExpectPrepare(insertSQL)
			mock.ExpectPrepare(dialect.CreateDeleteSQL(config))

			syncable := sql.New(db, config)
			err = syncable.Init()
			require.Nil(t, err)

			ctx := context.Background()

			for _, p := range createProposals(t, tt.data) {
				mock.ExpectBegin()
				shouldSnapshot, err := syncable.Sync(ctx, p)
				require.Nil(t, err)
				require.Equal(t, cluster.ShouldSnapshot(false), shouldSnapshot)
			}

			require.Nil(t, mock.ExpectationsWereMet())
		})
	}
}

func getDriverValues(vs []any) []driver.Value {
	ds := make([]driver.Value, 0, len(vs))

	for _, d := range vs {
		ds = append(ds, d)
	}

	return ds
}

func createProposals(t *testing.T, data [][]*Entity) []*cluster.Actual {
	ps := make([]*cluster.Actual, 0, len(data))
	for _, dataToProposal := range data {
		p := &cluster.Actual{}

		for _, dataToEntity := range dataToProposal {
			bs, err := json.Marshal(dataToEntity.Data)
			require.Nil(t, err)

			e := &cluster.Entity{
				Type: dataToEntity.Type,
				Key:  []byte(dataToEntity.Key),
				Data: bs,
			}
			p.Entities = append(p.Entities, e)
		}

		ps = append(ps, p)
	}

	return ps
}

type Entity struct {
	Type *cluster.Type
	Key  string
	Data any
	Args []any
}

func simpleEntity(key string, one string) *Entity {
	return &Entity{Type: simpleType, Key: key, Data: &Simple{Key: key, One: one}, Args: []any{key, one}}
}

func notSimpleEntity(key string, one string) *Entity {
	return &Entity{Type: notSimpleType, Key: key, Data: &Simple{Key: key, One: one}, Args: []any{key, one}}
}

type Simple struct {
	Key string `json:"key"`
	One string `json:"one"`
}

// --- Delete (right-to-be-forgotten) honoring ---

// newSimpleSyncable parses ./simple_syncable.toml, registers the DDL / insert
// / delete prepare expectations that Init issues (the config names a
// primaryKey, so a DELETE-by-key statement is prepared), and returns an Init'd
// syncable plus the insert and delete prepare handles for per-test exec
// expectations.
func newSimpleSyncable(t *testing.T, mock sqlmock.Sqlmock, dialect sql.Dialect, db *sql.DB) (
	*sql.Syncable, *sqlmock.ExpectedPrepare, *sqlmock.ExpectedPrepare,
) {
	t.Helper()

	bs, err := os.ReadFile("./simple_syncable.toml")
	require.Nil(t, err)
	v := readConfig(t, "toml", bytes.NewReader(bs))

	dbs := map[string]cluster.Database{"testdb": db}
	config, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
	require.Nil(t, err)

	mock.ExpectExec(dialect.CreateDDL(config)).WillReturnResult(driver.ResultNoRows)
	insertPrepare := mock.ExpectPrepare(dialect.CreateSQL(config))
	deletePrepare := mock.ExpectPrepare(dialect.CreateDeleteSQL(config))

	syncable := sql.New(db, config)
	require.Nil(t, syncable.Init())
	return syncable, insertPrepare, deletePrepare
}

func simpleJSON(t *testing.T, key, one string) []byte {
	t.Helper()
	bs, err := json.Marshal(&Simple{Key: key, One: one})
	require.Nil(t, err)
	return bs
}

// TestSyncUpsertThenDelete is the core honor-deletes path: an upsert writes the
// row, then a delete Actual for the same key issues a DELETE bound by the
// entity Key (no JSON unmarshal of the sentinel). It is also the zombie
// regression: a delete Actual no longer unmarshals invalid JSON into a
// permanent error / dead letter.
func TestSyncUpsertThenDelete(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	syncable, insertPrepare, deletePrepare := newSimpleSyncable(t, mock, dialect, db)
	ctx := context.Background()

	mock.ExpectBegin()
	insertPrepare.ExpectExec().WithArgs("key1", "one", "key1", "one").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	ss, err := syncable.Sync(ctx, &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(simpleType, []byte("key1"), simpleJSON(t, "key1", "one")),
	}})
	require.Nil(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)

	mock.ExpectBegin()
	deletePrepare.ExpectExec().WithArgs("key1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	ss, err = syncable.Sync(ctx, &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(simpleType, []byte("key1")),
	}})
	require.Nil(t, err)
	require.False(t, errors.Is(err, cluster.ErrPermanent), "delete must not be dead-lettered")
	require.Equal(t, cluster.ShouldSnapshot(true), ss)

	require.Nil(t, mock.ExpectationsWereMet())
}

// TestSyncDeleteAbsentKeyIsNoOp covers the bootstrap edge case: a delete for a
// row that was never inserted (e.g. a fresh syncable replaying an
// already-scrubbed log) affects zero rows but is still a success.
func TestSyncDeleteAbsentKeyIsNoOp(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	syncable, _, deletePrepare := newSimpleSyncable(t, mock, dialect, db)

	mock.ExpectBegin()
	deletePrepare.ExpectExec().WithArgs("ghost").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	ss, err := syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(simpleType, []byte("ghost")),
	}})
	require.Nil(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)
	require.Nil(t, mock.ExpectationsWereMet())
}

// TestSyncBatchMixedUpsertsAndDeletes verifies a single batch transaction
// applies upserts and deletes together, in order.
func TestSyncBatchMixedUpsertsAndDeletes(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	syncable, insertPrepare, deletePrepare := newSimpleSyncable(t, mock, dialect, db)

	mock.ExpectBegin()
	insertPrepare.ExpectExec().WithArgs("key1", "one", "key1", "one").
		WillReturnResult(sqlmock.NewResult(1, 1))
	deletePrepare.ExpectExec().WithArgs("key2").WillReturnResult(sqlmock.NewResult(0, 1))
	insertPrepare.ExpectExec().WithArgs("key3", "three", "key3", "three").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	as := []*cluster.Actual{
		{Entities: []*cluster.Entity{cluster.NewUpsertEntity(simpleType, []byte("key1"), simpleJSON(t, "key1", "one"))}},
		{Entities: []*cluster.Entity{cluster.NewDeleteEntity(simpleType, []byte("key2"))}},
		{Entities: []*cluster.Entity{cluster.NewUpsertEntity(simpleType, []byte("key3"), simpleJSON(t, "key3", "three"))}},
	}
	ss, err := syncable.SyncBatch(context.Background(), as)
	require.Nil(t, err)
	require.True(t, ss)
	require.Nil(t, mock.ExpectationsWereMet())
}

// TestSyncDeleteWithoutKeyColumnIsPermanent: a config that names neither
// keyColumn nor primaryKey cannot generate a delete, so a delete Actual fails
// permanently (a visible misconfiguration) rather than silently retaining the
// PII. Init prepares no delete statement in this case.
func TestSyncDeleteWithoutKeyColumnIsPermanent(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	config := &sql.Config{
		Topic:    "simple",
		Table:    "foo",
		Mappings: []sql.Mapping{{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}},
	}
	mock.ExpectExec(dialect.CreateDDL(config)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectPrepare(dialect.CreateSQL(config))

	syncable := sql.New(db, config)
	require.Nil(t, syncable.Init())

	mock.ExpectBegin()
	_, err = syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(simpleType, []byte("key1")),
	}})
	require.Error(t, err)
	require.True(t, errors.Is(err, cluster.ErrPermanent))
	require.Nil(t, mock.ExpectationsWereMet())
}
