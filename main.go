package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/philborlin/committed/api"

	"github.com/philborlin/committed/cluster"
	"github.com/philborlin/committed/raft"
)

func main() {
	nodeURLs := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	apiPort := flag.Int("port", 9121, "API server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var c *cluster.Cluster
	getSnapshot := func() ([]byte, error) { return c.GetSnapshot() }

	nodes := strings.Split(*nodeURLs, ",")
	dataDir := "data"
	if _, err := os.Stat(dataDir); err != nil {
		if err := os.MkdirAll(dataDir, 0750); err != nil {
			log.Fatal("cannot create data dir")
		}
	}
	commitC, errorC, snapshotterReady, leader := raft.NewRaftNode(
		*id, nodes, *join, dataDir, getSnapshot, proposeC, confChangeC)

	c = cluster.New(<-snapshotterReady, proposeC, commitC, errorC, dataDir, leader)

	api.ServeAPI(c, *apiPort, errorC)
}
