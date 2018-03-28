package db

import (
	"context"
	"testing"
)

// When create topic is called we want to startup a new cluster of raft nodes
func TestCreateTopic(t *testing.T) {
	topic := NewCluster().CreateTopic("", 3)

	defer topic.stop()

	if !topic.up() {
		t.Fatal("Topic not created")
	}
}

// When stop topic is called we want the underlying raft nodes to stop
func TestStopTopic(t *testing.T) {
	topic := NewCluster().CreateTopic("", 3)

	topic.stop()

	if topic.up() {
		t.Fatal("Topic did not stop correctly")
	}
}

func TestGetConfigTopic(t *testing.T) {
	configTopic := NewCluster().config()

	if configTopic == nil {
		t.Fatalf("No Config topic setup")
	}
}

func TestAppendToCluster(t *testing.T) {
	c := NewCluster()
	topicName := ""
	nodeCount := 3
	topic := c.CreateTopic(topicName, nodeCount)
	proposal := "Hello World"

	defer topic.stop()

	c.Append(context.TODO(), topicName, proposal)

	if !waitCommitConverge(topic.Nodes, uint64(nodeCount+1)) {
		t.Fatal("Commits did not converge")
	}

	data := topic.ReadIndex(context.TODO(), 0)

	if data != proposal {
		t.Fatalf("Expected [%v] but was [%v]", proposal, data)
	}
}
