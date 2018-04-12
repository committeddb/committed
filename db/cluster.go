package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/philborlin/committed/util"
)

// Cluster is an engine that spawns and maintains topics
// Cluster is a controller that manages nodes and balances resources among them
type Cluster struct {
	topics    map[string]*Topic
	nodes     []string
	httpstopc chan struct{}
	httpdonec chan struct{}
	transport *MultiTransport
}

// NewCluster creates a new cluster
func NewCluster(nodes []string, port int) *Cluster {
	c := &Cluster{
		topics:    make(map[string]*Topic),
		nodes:     nodes,
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),
	}

	go func() {
		addr := fmt.Sprintf(":%d", port)
		mux := http.NewServeMux()
		c.transport = NewMultiTransport(mux)
		mux.Handle("/node/topics", newNodeTopicHandler(c))
		mux.Handle("/cluster/topics", newClusterTopicHandler(c))
		mux.Handle("/cluster/topics/posts", newClusterTopicPostHandler(c))
		listener, err := newStoppableListener(addr, c.httpstopc)
		if err != nil {
			log.Fatalf("committed: Failed to create listener in API http (%v)", err)
		}

		log.Printf("Starting API http server on: %s\n", addr)
		err = (&http.Server{Handler: mux}).Serve(listener)
		if err != nil {
			log.Fatalf("committed: Failed to serve http in API http (%v)", err)
		}

		select {
		case <-c.httpstopc:
		default:
			log.Fatalf("committed: Failed to serve http in API http (%v)", err)
		}
		close(c.httpdonec)
	}()

	// TODO Wait for all nodes to startup?

	return c
}

// CreateTopic creates a new topic with node (replica) count
// TODO We can create a callback so the caller can get access to the topic if needed
func (c *Cluster) CreateTopic(name string, nodeCount int) error {
	// TODO We want some error handling if nodeCount > nodes
	fmt.Printf("CreateTopic [%s] %d\n", name, nodeCount)
	requestTopic(name, c.nodes)
	return nil
}

func (c *Cluster) createTopicCallback(t *Topic) {
	c.topics[t.Name] = t
}

func (c *Cluster) config() {
	name := "config"
	if c.topics[name] == nil {
		requestTopic(name, c.nodes)
	}
}

// Append finds the appropriate topic and then appends the proposal to that topic
func (c *Cluster) Append(ctx context.Context, topic string, proposal string) {
	log.Printf("Appending %v to %s\n", c.topics[topic], proposal)
	c.topics[topic].proposeC <- proposal
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
		c.c.CreateTopic(n.Name, n.NodeCount)
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
	if r.Method == "POST" {
		n := newClusterTopicPostRequest{}
		util.Unmarshall(r, &n)
		c.c.Append(context.TODO(), n.Topic, n.Proposal)
		w.Write(nil)
	}
}
