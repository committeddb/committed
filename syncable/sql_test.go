package syncable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/ioutil"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/philborlin/committed/types"
)

func TestCreateSQL(t *testing.T) {
	var mappings []sqlMapping
	mappings = append(mappings, sqlMapping{jsonPath: "", column: "bar", sqlType: "TEXT"})
	mappings = append(mappings, sqlMapping{jsonPath: "", column: "baz", sqlType: "TEXT"})

	expected := "INSERT INTO foo(bar,baz) VALUES ($1,$2)"
	actual := createSQL("foo", mappings)

	if expected != actual {
		t.Fatalf("Expected %v but was %v", expected, actual)
	}
}

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
		err = syncable.Init()
		Expect(err).To(BeNil())
	})

	It("should put values into the db", func() {
		defer func() {
			err = syncable.Close()
			Expect(err).To(BeNil())
		}()

		data := testReturn{Key: "lock", One: "two"}
		bytes, err := json.Marshal(&data)
		Expect(err).To(BeNil())

		syncable.Sync(context.Background(), bytes)

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
