package syncable

import (
	"bytes"
	"context"
	"os"
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

	defer delete()

	err = s.Db.Close()
	if err != nil {
		t.Fatalf("Failed with error %v", err)
	}
	if s.Db.Path() != "" {
		t.Fatal("Database failed to close")
	}
}

func TestSync(t *testing.T) {
	s, _ := NewBBolt("my.db", "keyName", "bucket")
	defer s.Close()
	defer delete()

	key := "key"
	value := "{\"keyName\": \"key\",\"value\": \"value\"}"
	s.Sync(context.TODO(), []byte(value))

	view(key, value, s.Bucket, s.Db, t)
}

func TestSyncMultipleItems(t *testing.T) {
	s, _ := NewBBolt("my.db", "keyName", "bucket")
	defer s.Close()
	defer delete()

	v1 := "{\"keyName\": \"k1\",\"value\": \"v1\"}"
	v2 := "{\"keyName\": \"k2\",\"value\": \"v2\"}"

	s.Sync(context.TODO(), []byte(v1))
	s.Sync(context.TODO(), []byte(v2))

	view("k1", v1, s.Bucket, s.Db, t)
	view("k2", v2, s.Bucket, s.Db, t)
}

func TestClose(t *testing.T) {
	defer delete()

	s, _ := NewBBolt("my.db", "", "bucket")

	if s.Db.Path() == "" {
		t.Fatal("Database is not open")
	}

	s.Close()

	if s.Db.Path() != "" {
		t.Fatal("Database failed to close")
	}
}

func view(key string, value string, bucket string, db *bolt.DB, t *testing.T) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		v := b.Get([]byte(key))
		if !bytes.Equal(v, []byte(value)) {
			t.Fatalf("Expected %v but was %v", value, v)
		}
		return nil
	})
}

func delete() {
	os.Remove("my.db")
}
