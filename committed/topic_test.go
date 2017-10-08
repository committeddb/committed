package committed

import (
	"context"
	"fmt"
	"testing"
)

// Append should propose a change to raft and continue to propose it until a timeout period
func TestAppend(t *testing.T) {
	fmt.Println("************** TEST APPEND ***************")
	topic := NewCluster().CreateTopic(3)
	proposal := "Hello World"

	defer topic.stop()

	topic.Append(context.TODO(), proposal)

	if !waitCommitConverge(topic.Nodes, 4) {
		t.Fatal("Commits did not converge")
	}
	fmt.Println("Commits converged")

	data := topic.ReadIndex(context.TODO(), 0)

	if data != proposal {
		t.Fatalf("Expected [%v] but was [%v]", proposal, data)
	}
}

// func findLeader(topic *Topic) raft.Node {
// 	var l map[uint64]struct{}
// 	var lindex int

// 	for {
// 		l = make(map[uint64]struct{})

// 		for i, n := range topic.nodes {
// 			lead := n.Status().SoftState.Lead
// 			// fmt.Printf("Lead: %+v\n\n", lead)
// 			// fmt.Printf("Id: %+v\n\n", n.Status().ID)
// 			if lead != 0 {
// 				l[lead] = struct{}{}

// 				if n.Status().ID == lead {
// 					lindex = i
// 				}
// 			}
// 		}

// 		if len(l) == 1 {
// 			fmt.Printf("Found Leader %+v\n\n", topic.nodes[lindex])
// 			return topic.nodes[lindex]
// 		}
// 	}

// var l raft.Node

// for i := 0; i < len(topic.nodes); i++ {
// 	node := topic.nodes[i]
// 	status := node.Status().SoftState.RaftState.String()

// 	fmt.Println("Node Status: " + status)

// 	if status == "StateLeader" {
// 		l = node
// 	}
// }

// return l

// return topic.nodes[0]
// }

// // Hook up a Ramsql connector and test syncing to the database
// func TestSyncToRamsql(t *testing.T) {

// }

// // Hook up a Bolt connector and test synching to the database
// func TestSyncToBolt(t *testing.T) {

// }
