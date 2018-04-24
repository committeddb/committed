package db

import (
	"context"
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

// HTTPAPI is a placeholder that allows us to shutdown the HTTP API
type httpAPI struct {
	server *http.Server
}

// serveAPI starts the committed API.
func serveAPI(c *Cluster, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) *httpAPI {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: createMux(c),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("HTTP API error: %v", err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Printf("HTTP API error: %v", err)
	}

	return &httpAPI{&srv}
}

// Shutdown shuts the server down
func (a *httpAPI) Shutdown() error {
	return a.server.Shutdown(context.Background())
}
