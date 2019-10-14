package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/types"
)

type testReturn struct {
	Key string
	One string
}

var _ = Describe("SQL Syncable", func() {
	var (
		data []byte
		err  error
		dbs  map[string]syncable.Database
		sync *Syncable
	)

	JustBeforeEach(func() {
		data, err = ioutil.ReadFile("./simple.toml")
		Expect(err).To(BeNil())
		dbs, err = databases()
		Expect(err).To(BeNil())
		_, parsed, err := syncable.ParseSyncable("toml", bytes.NewReader(data), dbs)
		Expect(err).To(BeNil())

		sync = parsed.(*Syncable)
		// RamSql does not support indexes
		sync.config.indexes = nil
		// RamSql does not support "if not exists" for tables
		err = sync.init(true)
		Expect(err).To(BeNil())
	})

	Describe("Dialect", func() {
		var (
			dialect  Dialect
			expected string
		)

		JustBeforeEach(func() {
			var mappings []sqlMapping
			mappings = append(mappings, sqlMapping{jsonPath: "", column: "bar", sqlType: "TEXT"})
			mappings = append(mappings, sqlMapping{jsonPath: "", column: "baz", sqlType: "TEXT"})

			fmt.Printf("Dialect: %v - SQL: [%s]\n", dialect, expected)
			actual := dialect.CreateSQL("foo", mappings)
			Expect(expected).To(Equal(actual))
		})

		Context("mysql", func() {
			BeforeEach(func() {
				dialect = &mySQLDialect{}
				expected = "INSERT INTO foo(bar,baz) VALUES ($1,$2) ON DUPLICATE KEY UPDATE bar=$1,baz=$2"
			})

			It("should create mysql sql", func() {})
		})

		Context("ramsql", func() {
			BeforeEach(func() {
				dialect = &ramsqlDialect{}
				expected = "INSERT INTO foo(bar,baz) VALUES ($1,$2)"
			})
			It("should create ramsql sql", func() {})
		})
	})

	It("should put values into the db", func() {
		defer func() {
			err = sync.Close()
			Expect(err).To(BeNil())
		}()

		data := testReturn{Key: "lock", One: "two"}
		bytes, err := json.Marshal(&data)
		Expect(err).To(BeNil())

		entry := &types.AcceptedProposal{Data: bytes}
		sync.Sync(context.Background(), entry)

		var value testReturn
		err = SelectOneRowFromDB(sync.DB, "SELECT * FROM foo", &value.Key, &value.One)
		Expect(err).To(BeNil())
		Expect(value.Key).To(Equal("lock"))
		Expect(value.One).To(Equal("two"))
	})
})

func SelectOneRowFromDB(db *sql.DB, table string, dest ...interface{}) error {
	rows, err := db.Query("SELECT * FROM foo")
	if err != nil {
		return err
	}
	defer rows.Close()
	var rowCount int
	for rows.Next() {
		rowCount++
		if err := rows.Scan(dest...); err != nil {
			return err
		}
	}
	if rowCount != 1 {
		return errors.New("Data is not in the database")
	}

	return rows.Err()
}
