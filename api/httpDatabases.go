package api

import (
	"encoding/json"
	"net/http"

	"github.com/philborlin/committed/db"
	"github.com/philborlin/committed/types"
	"github.com/philborlin/committed/util"
)

type newClusterDatabaseRequest struct {
	Name string
	Type string
	JSON string
}

type clusterDatabasesGetResponse struct {
	Databases map[string]types.Database
}

// NewClusterDatabaseHandler is the handler for Cluster Databases
func NewClusterDatabaseHandler(c *db.Cluster) http.Handler {
	return &clusterDatabaseHandler{c}
}

type clusterDatabaseHandler struct {
	c *db.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterDatabaseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		n := newClusterDatabaseRequest{}
		util.Unmarshall(r, &n)

		if n.Type == "SQL" {
			database := &types.SQLDB{}
			if err := json.Unmarshal([]byte(n.JSON), database); err != nil {
				http.Error(w, "", 500)
			}
			c.c.CreateDatabase(n.Name, database)
		}

		w.Write(nil)
	} else if r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterDatabasesGetResponse{c.c.Databases})
		w.Write(response)
	}
}
