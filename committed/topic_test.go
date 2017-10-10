package committed

import (
	"bytes"
	"context"
	"testing"

	bolt "github.com/coreos/bbolt"
	"github.com/philborlin/committed/committed/syncable"
)

// Append should propose a change to raft and continue to propose it until a timeout period
func TestAppend(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic(nodeCount)
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

// // Hook up a Ramsql connector and test syncing to the database
// func TestSyncToRamsql(t *testing.T) {

// }

// TestHistoricalSync tests whether an item that has been committed prior to the sync action is persisted by the syncable
func TestHistoricalSync(t *testing.T) {
	nodeCount := 3
	topic := NewCluster().CreateTopic(nodeCount)
	s, _ := syncable.NewBBolt("my.db", "", "bucket")
	defer topic.stop()
	defer s.Close()

	value := "{\"keyName\": \"key\",\"value\": \"value\"}"
	topic.Append(context.TODO(), value)
	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+1)) {
		t.Fatal("Commits did not converge")
	}

	topic.Sync(context.TODO(), s)

	s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))
		v := b.Get([]byte("key"))
		if !bytes.Equal(v, []byte(value)) {
			t.Fatalf("Expected %v but was %v", value, v)
		}
		return nil
	})
}

// Hook up a Bolt connector and test synching to the database
// func TestSyncToBolt(t *testing.T) {
// 	nodeCount := 3
// 	dbName := "my.db"
// 	topic := NewCluster().CreateTopic(nodeCount)
// 	defer topic.stop()

// 	keyName := "keyName"
// 	key := "key"
// 	value := "{\"keyName\": \"key\",\"value\": \"value\"}"

// 	topic.Append(context.TODO(), value)
// 	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+1)) {
// 		t.Fatal("Commits did not converge")
// 	}

// 	db, err := bolt.Open(dbName, 0600, nil)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer db.Close()
// 	defer os.Remove(dbName)

// 	bucket := "MyBucket"
// 	topic.Sync(context.TODO(), &BBolt{Db: db, KeyName: keyName, Bucket: bucket})

// 	db.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucket))
// 		if b == nil {
// 			t.Fatal("Bucket does not exist")
// 		}
// 		v := b.Get([]byte(key))
// 		if !bytes.Equal(v, []byte(value)) {
// 			t.Fatalf("Expected [%v] but was [%v]", value, v)
// 		}
// 		return nil
// 	})
// }
