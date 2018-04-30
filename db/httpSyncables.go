package db

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"

	"github.com/philborlin/committed/util"
)

type newClusterSyncableRequest struct {
	Style    string
	Syncable string
}

type clusterSyncableGetResponse struct {
	Syncables []string
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
		decoded, err := base64.StdEncoding.DecodeString(n.Syncable)
		if err != nil {
			w.WriteHeader(500)
		} else {
			c.c.CreateSyncable(n.Style, string(decoded))
			w.Write(nil)
		}
	} else if r.Method == "GET" {
		keys := make([]string, 0, len(c.c.syncables))
		for _, key := range c.c.syncables {
			keys = append(keys, key)
		}
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterSyncableGetResponse{keys})
		w.Write(response)
	}
}
