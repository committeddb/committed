package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/philborlin/committed/cluster"
)

func createMux(c *cluster.Cluster) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/cluster/databases", newLoggingHandler(NewClusterDatabaseHandler(c)))
	mux.Handle("/cluster/posts", newLoggingHandler(NewClusterPostHandler(c)))
	mux.Handle("/cluster/syncables", newLoggingHandler(NewClusterSyncableHandler(c)))
	mux.Handle("/cluster/topics", newLoggingHandler(NewClusterTopicHandler(c)))
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
func ServeAPI(c *cluster.Cluster, port int, errorC <-chan error) *HTTPAPI {
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
		log.Fatalf("Shutting down http server: Raft error: %v", err)
	}

	return &HTTPAPI{&srv}
}

// Shutdown shuts the server down
func (a *HTTPAPI) Shutdown() error {
	return a.server.Shutdown(context.Background())
}
