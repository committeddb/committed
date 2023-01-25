package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/philborlin/committed/internal/node/api"

	"github.com/philborlin/committed/internal/node/cluster"
	"github.com/philborlin/committed/internal/node/raft"

	// Ensures that init() functions run
	_ "github.com/philborlin/committed/internal/node/syncable"
	_ "github.com/philborlin/committed/internal/node/syncable/file"
	_ "github.com/philborlin/committed/internal/node/syncable/sql"
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
	baseDir := "data"
	if _, err := os.Stat(baseDir); err != nil {
		if err := os.MkdirAll(baseDir, 0750); err != nil {
			log.Fatal("cannot create data dir")
		}
	}
	commitC, errorC, snapshotterReady, leader := raft.NewRaftNode(
		*id, nodes, *join, baseDir, getSnapshot, proposeC, confChangeC)

	c = cluster.New(<-snapshotterReady, proposeC, commitC, errorC, baseDir, leader, *id)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("Received a signal to shutdown. Shutting down...")
		err := c.Shutdown()
		if err != nil {
			log.Println(err)
		}
		log.Println("Graceful shutdown complete. Exiting...")
		os.Exit(0)
	}()

	api.ServeAPI(c, *apiPort, errorC)
}
