package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/rakyll/statik/fs"

	"github.com/philborlin/committed/db"
	_ "github.com/philborlin/committed/statik" // This imports the static http files for the React App
)

func createMux(c *db.Cluster) http.Handler {
	fs, err := fs.New()
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/cluster/databases", newLoggingHandler(NewClusterDatabaseHandler(c)))
	mux.Handle("/cluster/posts", newLoggingHandler(NewClusterPostHandler(c)))
	mux.Handle("/cluster/syncables", newLoggingHandler(NewClusterSyncableHandler(c)))
	mux.Handle("/cluster/topics", newLoggingHandler(NewClusterTopicHandler(c)))
	mux.Handle("/", http.FileServer(fs))
	return mux
}

type loggingHandler struct {
	handler http.Handler
}

func newLoggingHandler(handler http.Handler) http.Handler {
	return &loggingHandler{handler: handler}
}

// ServeHTTP implements Handler
func (h *loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received: %s %s\n", r.Method, r.RequestURI)
	fmt.Printf("%v -> %v", r.RemoteAddr, r.RequestURI)
	h.handler.ServeHTTP(w, r)
}

// HTTPAPI is a placeholder that allows us to shutdown the HTTP API
type HTTPAPI struct {
	server *http.Server
}

// ServeAPI starts the committed API.
func ServeAPI(c *db.Cluster, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) *HTTPAPI {
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

	return &HTTPAPI{&srv}
}

// Shutdown shuts the server down
func (a *HTTPAPI) Shutdown() error {
	return a.server.Shutdown(context.Background())
}
