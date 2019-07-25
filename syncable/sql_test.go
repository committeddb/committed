package syncable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/ioutil"

	"github.com/coreos/etcd/raft/raftpb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/philborlin/committed/types"
)

type testReturn struct {
	Key string
	One string
}

var _ = Describe("SQL Syncable", func() {
	var (
		data     []byte
		err      error
		dbs      map[string]types.Database
		syncable *SQLSyncable
	)

	JustBeforeEach(func() {
		data, err = ioutil.ReadFile("./simple.toml")
		Expect(err).To(BeNil())
		dbs, err = databases()
		Expect(err).To(BeNil())
		_, parsed, err := Parse("toml", bytes.NewReader(data), dbs)
		Expect(err).To(BeNil())

		syncable = parsed.(*SQLSyncable)
		// RamSql does not support indexes
		syncable.config.indexes = nil
		// RamSql does not support "if not exists" for tables
		err = syncable.init(true)
		Expect(err).To(BeNil())
	})

	It("should create sql", func() {
		var mappings []sqlMapping
		mappings = append(mappings, sqlMapping{jsonPath: "", column: "bar", sqlType: "TEXT"})
		mappings = append(mappings, sqlMapping{jsonPath: "", column: "baz", sqlType: "TEXT"})

		expected := "INSERT INTO foo(bar,baz) VALUES ($1,$2)"
		actual := createSQL("foo", mappings)

		Expect(expected).To(Equal(actual))
	})

	It("should put values into the db", func() {
		defer func() {
			err = syncable.Close()
			Expect(err).To(BeNil())
		}()

		data := testReturn{Key: "lock", One: "two"}
		bytes, err := json.Marshal(&data)
		Expect(err).To(BeNil())

		entry := raftpb.Entry{Data: bytes}
		syncable.Sync(context.Background(), entry)

		var value testReturn
		err = SelectOneRowFromDB(syncable.DB, "SELECT * FROM foo", &value.Key, &value.One)
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
