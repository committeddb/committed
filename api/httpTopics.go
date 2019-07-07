package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/philborlin/committed/db"
	"github.com/philborlin/committed/util"
)

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

type clusterTopicGetResponse struct {
	Topics []string
}

// NewClusterTopicHandler is the handler for Cluster Topics
func NewClusterTopicHandler(c *db.Cluster) http.Handler {
	return &clusterTopicHandler{c}
}

type clusterTopicHandler struct {
	c *db.Cluster
}

func (c *clusterTopicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	log.Printf("Received: %s %s\n", r.Method, r.RequestURI)
	if r.Method == "POST" {
		n := newClusterTopicRequest{}
		util.Unmarshall(r, &n)
		c.c.CreateTopic(n.Name)
		w.Write(nil)
	} else if r.Method == "GET" {
		log.Printf("Processing GET\n")
		log.Printf("Found topics %v GET\n", c.c.Topics)
		keys := make([]string, 0, len(c.c.Topics))
		for key := range c.c.Topics {
			keys = append(keys, key)
		}
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterTopicGetResponse{keys})
		w.Write(response)
	}
}
