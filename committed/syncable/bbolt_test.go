package syncable

import (
	"bytes"
	"context"
	"testing"

	bolt "github.com/coreos/bbolt"
)

func TestCreate(t *testing.T) {
	s, err := NewBBolt("my.db", "", "bucket")
	if err != nil {
		t.Fatalf("Failed with error %v", err)
	}
	if s.Db.Path() == "" {
		t.Fatal("Database is not open")
	}

	err = s.Db.Close()
	if err != nil {
		t.Fatalf("Failed with error %v", err)
	}
	if s.Db.Path() != "" {
		t.Fatal("Database failed to close")
	}
}

func TestSynch(t *testing.T) {
	s, _ := NewBBolt("my.db", "", "bucket")
	defer s.Close()

	key := "key"
	value := "{\"keyName\": \"key\",\"value\": \"value\"}"
	s.Sync(context.TODO(), []byte(value))

	s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))
		v := b.Get([]byte(key))
		if !bytes.Equal(v, []byte(value)) {
			t.Fatalf("Expected %v but was %v", value, v)
		}
		return nil
	})
}

func TestClose(t *testing.T) {
	s, _ := NewBBolt("my.db", "", "bucket")

	if s.Db.Path() == "" {
		t.Fatal("Database is not open")
	}

	s.Close()

	if s.Db.Path() != "" {
		t.Fatal("Database failed to close")
	}
}

// TODO We should delete the database between runs by maybe using - defer os.Remove(dbName)
