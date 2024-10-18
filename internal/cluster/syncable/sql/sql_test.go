package sql_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"os"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
	"github.com/stretchr/testify/require"

	"github.com/DATA-DOG/go-sqlmock"
)

var simpleType = &cluster.Type{ID: "simple", Name: "simple"}
var notSimpleType = &cluster.Type{ID: "notSimple", Name: "notSimple"}

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
			insertSQL := dialect.CreateSQL(config.Table, config.Mappings)
			expectedPrepare := mock.ExpectPrepare(insertSQL)

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
			insertSQL := dialect.CreateSQL(config.Table, config.Mappings)
			mock.ExpectPrepare(insertSQL)

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
	var ds []driver.Value

	for _, d := range vs {
		ds = append(ds, d)
	}

	return ds
}

func createProposals(t *testing.T, data [][]*Entity) []*cluster.Proposal {
	var ps []*cluster.Proposal
	for _, dataToProposal := range data {
		p := &cluster.Proposal{}

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
