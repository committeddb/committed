package syncable

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/ioutil"
	"testing"
)

func TestCreateSQL(t *testing.T) {
	var mappings []sqlMapping
	mappings = append(mappings, sqlMapping{"", "foo", "bar"})
	mappings = append(mappings, sqlMapping{"", "foo", "baz"})

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

func TestSyncPutsValuesInDB(t *testing.T) {
	dat, err := ioutil.ReadFile("./simple.toml")
	if err != nil {
		t.Fatalf("Failed with error %v", err)
	}

	parsed, _ := Parse("toml", dat)
	syncable := parsed.(*syncableWrapper).Syncable.(*SQLSyncable)
	defer syncable.Close()

	execInTransaction(syncable.DB, "CREATE TABLE foo (key string, two string);", t)

	data := testReturn{Key: "lock", One: "two"}
	bytes, err := json.Marshal(&data)
	if err != nil {
		t.Fatalf("Failed to parse %s: %v", data, err)
	}

	syncable.Sync(context.Background(), bytes)

	var value testReturn
	err = SelectOneRowFromDB(syncable.DB, "SELECT * FROM foo", &value.Key, &value.One)
	if err != nil {
		t.Fatal(err)
	}
	if value.Key != "lock" || value.One != "two" {
		t.Fatalf("Expected %v but was %v", data, value)
	}
}

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

func execInTransaction(db *sql.DB, sqlString string, t *testing.T) {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: 0, ReadOnly: false})
	if err != nil {
		t.Fatalf("Failed to Begin Transaction: %v", err)
	}

	_, err = tx.ExecContext(context.Background(), sqlString)
	if err != nil {
		t.Fatalf("Failed to create table foo: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
