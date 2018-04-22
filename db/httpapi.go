package db

import (
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
)

func createMux(c *Cluster) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/cluster/topics", NewClusterTopicHandler(c))
	mux.Handle("/cluster/posts", NewClusterPostHandler(c))
	mux.Handle("/cluster/syncables", NewClusterSyncableHandler(c))
	return mux
}

// serveAPI starts the committed API.
func serveAPI(c *Cluster, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: createMux(c),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
