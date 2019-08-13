package api

import (
	"encoding/json"
	"net/http"

	"github.com/philborlin/committed/cluster"
	"github.com/philborlin/committed/types"
)

type clusterDatabasesGetResponse struct {
	Databases map[string]types.Database
}

// NewClusterDatabaseHandler is the handler for Cluster Databases
func NewClusterDatabaseHandler(c *cluster.Cluster) http.Handler {
	return &clusterDatabaseHandler{c}
}

type clusterDatabaseHandler struct {
	c *cluster.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterDatabaseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		toml, err := ReaderToString(r.Body)
		if err != nil {
			ErrorTo500(w, err)
			return
		}

		err = c.c.ProposeDatabase(toml)
		if err != nil {
			ErrorTo500(w, err)
			return
		}
		w.Write(nil)
	} else if r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterDatabasesGetResponse{c.c.Data.Databases})
		w.Write(response)
	}
}
