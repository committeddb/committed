package db

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/philborlin/committed/util"
)

func clusterTest(t *testing.T, f func(*Cluster) (expected interface{}, actual interface{}, err error)) {
	c := NewCluster([]string{"http://127.0.0.1:12379"}, 1, 12380, false)

	time.AfterFunc(2*time.Second, func() {
		log.Printf("Starting test function")
		defer c.Shutdown()
		defer os.RemoveAll(fmt.Sprintf("raft-%d", 1))
		defer os.RemoveAll(fmt.Sprintf("raft-%d-snap", 1))

		expected, actual, err := f(c)

		log.Printf("[%v][%v][%v]", expected, actual, err)

		if err != nil {
			t.Fatalf("Error: %v", err)
		}

		if expected != actual {
			t.Fatalf("Expected %v but was %v", expected, actual)
		}

		time.Sleep(2 * time.Second)
	})

	c.Start()
}

func TestCreateTopic(t *testing.T) {
	f := func(c *Cluster) (interface{}, interface{}, error) {
		topicName := "test1"
		expected := c.CreateTopic(topicName)
		actual := c.topics[topicName]
		return expected, actual, nil
	}

	clusterTest(t, f)
}

func TestAppendToTopic(t *testing.T) {
	f := func(c *Cluster) (interface{}, interface{}, error) {
		expected := util.Proposal{Topic: "test1", Proposal: "Hello World"}
		c.Append(expected)
		time.Sleep(2 * time.Second)
		log.Printf("About to check storage: %v", c.storage)
		lastIndex, err := c.storage.LastIndex()
		if err != nil {
			t.Fatalf("Error: %v", err)
		}

		entries, err := c.storage.Entries(lastIndex, lastIndex+1, 1)
		if err != nil {
			t.Fatalf("Error: %v", err)
		}

		actual, err := decodeProposal(entries[0].Data)

		return expected, actual, err
	}

	clusterTest(t, f)
}

func TestAddSQLSyncableToCluster(t *testing.T) {
	f := func(c *Cluster) (interface{}, interface{}, error) {

		return nil, nil, nil
	}

	clusterTest(t, f)
}
