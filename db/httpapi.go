package db

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/philborlin/committed/util"
)

func createMux(c *Cluster) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/cluster/topics", newClusterTopicHandler(c))
	mux.Handle("/cluster/posts", newClusterTopicPostHandler(c))
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

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

type clusterTopicGetResponse struct {
	Topics []string
}

func newClusterTopicHandler(c *Cluster) http.Handler {
	return &clusterTopicHandler{c}
}

type clusterTopicHandler struct {
	c *Cluster
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
		log.Printf("Found topics %v GET\n", c.c.topics)
		keys := make([]string, 0, len(c.c.topics))
		for key := range c.c.topics {
			keys = append(keys, key)
		}
		response, _ := json.Marshal(clusterTopicGetResponse{keys})
		w.Write(response)
	}
}

type newClusterTopicPostRequest struct {
	Topic    string
	Proposal string
}

func newClusterTopicPostHandler(c *Cluster) http.Handler {
	return &clusterTopicPostHandler{c}
}

type clusterTopicPostHandler struct {
	c *Cluster
}

func (c *clusterTopicPostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	log.Printf("Received: %s %s\n", r.Method, r.RequestURI)
	if r.Method == "POST" {
		n := newClusterTopicPostRequest{}
		util.Unmarshall(r, &n)
		c.c.Append(Proposal{n.Topic, n.Proposal})
		w.Write(nil)
	}
}
