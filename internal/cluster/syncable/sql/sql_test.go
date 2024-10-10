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

func TestSync(t *testing.T) {
	simpleOne := simpleEntity("key1", "one")
	simpleTwo := simpleEntity("key2", "two")
	simpleThree := simpleEntity("key3", "three")

	tests := []struct {
		name           string
		data           [][]*Entity
		configFileName string
	}{
		{"one-simple", [][]*Entity{{simpleOne}}, "./simple.toml"},
		{"two-simple", [][]*Entity{{simpleOne, simpleTwo}}, "./simple.toml"},
		{"two-one-simple", [][]*Entity{{simpleOne, simpleTwo}, {simpleThree}}, "./simple.toml"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlMockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.Nil(t, err)

			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)
			config := sql.Parse("toml", bytes.NewReader(bs))

			dialect := &dialects.SQLMockDialect{}
			db := sql.NewDB(dialect, sqlMockDB)
			defer db.Close()

			insertSQL := dialect.CreateSQL(config.Table, config.Mappings)
			expectedPrepare := mock.ExpectPrepare(insertSQL)

			syncable := sql.New(db, dialect, config)
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
					expectedPrepare.ExpectExec().WithArgs(dvs...).WillReturnResult(result)
				}
				mock.ExpectCommit()
				err := syncable.Sync(ctx, p)
				require.Nil(t, err)
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

type Simple struct {
	Key string `json:"key"`
	One string `json:"one"`
}
