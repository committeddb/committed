package syncable

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
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
	syncable := parsed.(*sqlSyncable)
	defer syncable.Close()

	execInTransaction(syncable.db, "CREATE TABLE foo (key string, two string);", t)

	data := testReturn{Key: "lock", One: "two"}
	bytes, err := json.Marshal(&data)
	if err != nil {
		t.Fatalf("Failed to parse %s: %v", data, err)
	}

	syncable.Sync(context.Background(), bytes)

	rows, err := syncable.db.Query("SELECT * FROM foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var rowCount int
	for rows.Next() {
		rowCount++
		var value testReturn
		if err := rows.Scan(&value.Key, &value.One); err != nil {
			t.Fatal(err)
		}
		if value.Key != "lock" || value.One != "two" {
			t.Fatalf("Expected %v but was %v", testReturn{"lock", "two"}, value)
		}
	}
	if rowCount != 1 {
		t.Fatalf("Data is not in the database")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
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
