package committed

import (
	"context"
	"testing"
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

// // Hook up a Bolt connector and test synching to the database
// func TestSyncToBolt(t *testing.T) {

// }
