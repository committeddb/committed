package db

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/philborlin/committed/committed/syncable"
)

// Append should propose a change to raft and continue to propose it until a timeout period
func TestAppend(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic("", nodeCount)
	proposal := "Hello World"

	defer topic.stop()

	topic.Append(context.TODO(), proposal)

	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+1)) {
		t.Fatal("Commits did not converge")
	}

	data := topic.ReadIndex(context.TODO(), 0)

	if data != proposal {
		t.Fatalf("Expected [%v] but was [%v]", proposal, data)
	}
}

// TestSize tests an internal size functions used during syncing
func TestSize(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic("", nodeCount)

	defer topic.stop()

	topic.Append(context.TODO(), "p1")
	topic.Append(context.TODO(), "p2")

	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+2)) {
		t.Fatal("Commits did not converge")
	}

	s := topic.size(context.TODO())
	if s != 2 {
		t.Fatalf("Expected size to be [2] but it was [%v]", s)
	}
}

// TestHistoricalSync tests whether an item that has been committed prior to the sync action is persisted by the syncable
func TestHistoricalSync(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic("", nodeCount)
	s, _ := syncable.NewBBolt("my.db", "keyName", "bucket")
	defer topic.stop()
	defer s.Close()

	value := "{\"keyName\": \"key\",\"value\": \"value\"}"
	topic.Append(context.TODO(), value)
	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+1)) {
		t.Fatal("Commits did not converge")
	}

	topic.Sync(context.TODO(), s)

	view("key", value, s.Bucket, s.Db, t)
}

func TestMultipleHistoricalSync(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic("", nodeCount)
	s, _ := syncable.NewBBolt("my.db", "keyName", "bucket")
	defer topic.stop()
	defer s.Close()
	defer delete()

	v1 := "{\"keyName\": \"k1\",\"value\": \"v1\"}"
	v2 := "{\"keyName\": \"k2\",\"value\": \"v2\"}"
	topic.Append(context.TODO(), v1)
	topic.Append(context.TODO(), v2)
	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+2)) {
		t.Fatal("Commits did not converge")
	}

	topic.Sync(context.TODO(), s)
	time.Sleep(50 * time.Millisecond)

	view("k1", v1, s.Bucket, s.Db, t)
	view("k2", v2, s.Bucket, s.Db, t)
}

func TestMultipleFutureSync(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic("", nodeCount)
	s, _ := syncable.NewBBolt("my.db", "keyName", "bucket")
	defer topic.stop()
	defer s.Close()
	defer delete()

	v1 := "{\"keyName\": \"k1\",\"value\": \"v1\"}"
	v2 := "{\"keyName\": \"k2\",\"value\": \"v2\"}"

	go func() { topic.Sync(context.TODO(), s) }()

	topic.Append(context.TODO(), v1)
	topic.Append(context.TODO(), v2)
	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+2)) {
		t.Fatal("Commits did not converge")
	}

	time.Sleep(time.Millisecond)

	view("k1", v1, s.Bucket, s.Db, t)
	view("k2", v2, s.Bucket, s.Db, t)
}

func TestMultipleFutureSyncsWithMultipleSyncables(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic("", nodeCount)
	s1, _ := syncable.NewBBolt("my.db", "keyName", "bucket")
	s2, _ := syncable.NewBBolt("my2.db", "keyName", "bucket")
	defer topic.stop()
	defer s1.Close()
	defer s2.Close()
	defer delete()
	defer os.Remove("my2.db")

	v1 := "{\"keyName\": \"k1\",\"value\": \"v1\"}"
	v2 := "{\"keyName\": \"k2\",\"value\": \"v2\"}"

	go func() { topic.Sync(context.TODO(), s1) }()
	go func() { topic.Sync(context.TODO(), s2) }()

	topic.Append(context.TODO(), v1)
	topic.Append(context.TODO(), v2)
	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+2)) {
		t.Fatal("Commits did not converge")
	}

	time.Sleep(2 * time.Millisecond)

	view("k1", v1, s1.Bucket, s1.Db, t)
	view("k2", v2, s1.Bucket, s1.Db, t)
	view("k1", v1, s2.Bucket, s2.Db, t)
	view("k2", v2, s2.Bucket, s2.Db, t)
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

// TODO Ideas - Close a syncable and stop the for loop
