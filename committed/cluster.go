package committed

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
)

// Cluster is an engine that spawns and maintains topics
// Cluster is a controller that manages nodes and balances resources among them
type Cluster struct {
	topics map[string]*Topic
	nodes  []string
}

// NewCluster creates a new cluster
func NewCluster(nodes []string) *Cluster {
	c := &Cluster{
		topics: make(map[string]*Topic),
		nodes:  nodes,
	}
	fmt.Printf("Created new cluster.\n")

	go func() {
		fmt.Printf("Adding http handlers.\n")
		http.Handle("/node/topics", newNodeTopicHandler(c))
		http.Handle("/cluster/topics", newClusterTopicHandler(c))
		http.Handle("/cluster/topics/posts", newClusterTopicPostHandler(c))
		fmt.Printf("Starting http server.\n")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			fmt.Printf("Panicking.\n")
			panic(err)
		}
	}()

	// TODO Wait for all nodes to startup?

	return c
}

// CreateTopic creates a new topic with node (replica) count
// TODO We can create a callback so the caller can get access to the topic if needed
func (c *Cluster) CreateTopic(name string, nodeCount int) error {
	// TODO We want some error handling if nodeCount > nodes
	fmt.Printf("CreateTopic [%s] %d\n", name, nodeCount)
	requestTopic(name, c.nodes, randomPort())
	return nil
}

func randomPort() int {
	max := 65535 - 49152 - 1
	return rand.Intn(max) + 49152 + 1
}

func (c *Cluster) createTopicCallback(t *Topic) {
	fmt.Printf("createTopicCallback [%v]\n", t.Name)
	c.topics[t.Name] = t
}

func (c *Cluster) config() {
	name := "config"
	if c.topics[name] == nil {
		requestTopic(name, c.nodes, 49152)
	}
}

// Append finds the appropriate topic and then appends the proposal to that topic
func (c *Cluster) Append(ctx context.Context, topic string, proposal string) {
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
	if r.Method == "POST" {
		n := newClusterTopicRequest{}
		unmarshall(r, &n)
		c.c.CreateTopic(n.Name, n.NodeCount)
	} else if r.Method == "GET" {
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
		unmarshall(r, &n)
		c.c.Append(context.TODO(), n.Topic, n.Proposal)
	}
}

func unmarshall(r *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	log.Printf("Body %v\n", string(body))
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &v)
	return err
}
