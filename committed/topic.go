package committed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
)

// Topic represents a named raft cluster that can be proposed to or synched
type Topic struct {
	Name        string
	commitC     <-chan *string
	errorC      <-chan error
	proposeC    chan string
	confChangeC <-chan raftpb.ConfChange
}

type newNodeTopicRequest struct {
	Name  string
	ID    int
	Peers []string
	Join  bool
}

func requestTopic(name string, nodes []string, port int) error {
	fmt.Printf("requestTopic [%s]\n", name)
	var peers []string
	for _, node := range nodes {
		u, _ := url.Parse(node)
		url := fmt.Sprintf("%s://%s:%d", u.Scheme, u.Hostname(), port)
		peers = append(peers, url)
	}

	var http = &http.Client{
		Timeout: time.Second * 10,
	}

	for i := 0; i < len(nodes); i++ {
		request, _ := json.Marshal(newNodeTopicRequest{Name: name, ID: i + 1, Peers: peers, Join: false})
		fmt.Printf("json: %s\n", string(request[:]))
		r := bytes.NewReader(request)
		resp, err := http.Post(fmt.Sprintf("%s/node/topics", nodes[i]), "application/json", r)
		if err != nil {
			fmt.Printf("%v\n", err)
			return err
		}
		fmt.Printf("requestTopic POST is successful\n")
		defer resp.Body.Close()
		// TODO Handle response
	}

	return nil
}

func newTopic(name string, id int, peers []string, join bool) *Topic {
	fmt.Printf("newTopic [%v]\n", name)
	proposeC := make(chan string)
	// defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	// defer close(confChangeC)

	commitC, errorC := newRaftNode(id, name, peers, join, proposeC, confChangeC)
	return &Topic{
		Name:        name,
		commitC:     commitC,
		errorC:      errorC,
		proposeC:    proposeC,
		confChangeC: confChangeC,
	}
}

// func (t *Topic) stop() {
// 	for i := 0; i < len(t.Nodes); i++ {
// 		t.Nodes[i].Stop()
// 	}
// }

// func (t *Topic) up() bool {
// 	v := false

// 	for i := 0; i < len(t.Nodes); i++ {
// 		if t.Nodes[i].Node.Status().ID > 0 {
// 			v = true
// 		}
// 	}

// 	return v
// }

// // Append a proposal to the topic
// func (t *Topic) Append(ctx context.Context, proposal string) {
// 	t.commitC <- proposal
// 	// n := t.Nodes[0]
// 	// n.Propose(ctx, []byte(proposal))
// }

// func (t *Topic) size(ctx context.Context) uint64 {
// 	storage := t.Nodes[0].storage
// 	first, _ := storage.FirstIndex()
// 	last, _ := storage.LastIndex()

// 	entries, error := storage.Entries(first, last+1, uint64(1024*1024))
// 	if error != nil {
// 		fmt.Println("[topic] Error retrieving entries from storage")
// 	}

// 	count := uint64(0)
// 	for _, e := range entries {
// 		if e.Type == raftpb.EntryNormal && len(e.Data) != 0 {
// 			count++
// 		}
// 	}

// 	return count
// }

// // ReadIndex from the topic
// func (t *Topic) ReadIndex(ctx context.Context, index uint64) string {
// 	storage := t.Nodes[0].storage
// 	first, _ := storage.FirstIndex()
// 	last, _ := storage.LastIndex()

// 	entries, error := storage.Entries(first, last+1, uint64(1024*1024))
// 	if error != nil {
// 		fmt.Println("[topic] Error retrieving entries from storage")
// 	}

// 	count := uint64(0)
// 	for _, e := range entries {
// 		if e.Type == raftpb.EntryNormal && len(e.Data) != 0 {
// 			if count == index {
// 				return string(e.Data[:])
// 			}
// 			count++
// 		}
// 	}

// 	// TODO This should be an error
// 	fmt.Println("[topic] Could not find index")
// 	return ""
// }

// TODO Add syncables
// // Sync the contents of the topic into a Syncable
// func (t *Topic) Sync(ctx context.Context, s syncable.Syncable) {
// 	size := t.size(ctx)

// 	for i := uint64(0); i < size; i++ {
// 		s.Sync(ctx, []byte(t.ReadIndex(ctx, uint64(i))))
// 	}

// 	for _, n := range t.Nodes {
// 		syncNode(ctx, s, n)
// 	}
// }

// func syncNode(ctx context.Context, s syncable.Syncable, n *node) {
// 	subc := n.syncp.Sub("StoredData")
// 	go func() {
// 		for {
// 			select {
// 			case e := <-subc:
// 				s.Sync(ctx, e.(raftpb.Entry).Data)
// 			default:
// 				time.Sleep(time.Millisecond * 1)
// 			}
// 		}
// 	}()
// }

func newNodeTopicHandler(c *Cluster) http.Handler {
	return &nodeTopicHandler{c}
}

type nodeTopicHandler struct {
	c *Cluster
}

func (c *nodeTopicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		n := newNodeTopicRequest{}
		unmarshall(r, &n)
		t := newTopic(n.Name, n.ID, n.Peers, n.Join)
		c.c.createTopicCallback(t)
	}
}

// func main() {
// 	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
// 	id := flag.Int("id", 1, "node ID")
// 	// kvport := flag.Int("port", 9121, "key-value server port")
// 	join := flag.Bool("join", false, "join an existing cluster")
// 	flag.Parse()

// 	proposeC := make(chan string)
// 	defer close(proposeC)
// 	confChangeC := make(chan raftpb.ConfChange)
// 	defer close(confChangeC)

// 	// raft provides a commit stream for the proposals from the http api
// 	// var kvs *kvstore
// 	// getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
// 	// commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)
// 	commitC, errorC := newRaftNode(*id, strings.Split(*cluster, ","), *join, proposeC, confChangeC)

// 	// kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

// 	// the key-value http handler will propose updates to raft
// 	// serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
// }
