package db

import (
	"log"
	"net/http"

	"github.com/philborlin/committed/util"
)

type newClusterSyncableRequest struct {
	Style    string
	Syncable string
}

// NewClusterSyncableHandler creates a new handler for Cluster Sycnables
func NewClusterSyncableHandler(c *Cluster) http.Handler {
	return &clusterSyncableHandler{c}
}

type clusterSyncableHandler struct {
	c *Cluster
}

func (c *clusterSyncableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	log.Printf("Received: %s %s\n", r.Method, r.RequestURI)
	if r.Method == "POST" {
		n := newClusterSyncableRequest{}
		util.Unmarshall(r, &n)
		c.c.CreateSyncable(n.Style, n.Syncable)
		w.Write(nil)
	}
}
