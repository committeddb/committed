//go:build docker

package harness

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// collector is the harness's observation point for the committed log.
//
// The committed log is write-only over HTTP by design — there is no
// GET /proposal — so the harness observes it the way a real consumer does:
// it stands up an HTTP endpoint and registers one webhook syncable per topic
// that POSTs every committed Actual to it. The collector records those
// Actuals per topic in commit order, deduplicated by raft index (the
// Idempotency-Key) so an at-least-once redelivery (e.g. after a restart)
// isn't double-counted. snapshotCounts and Capture read back from here.
type collector struct {
	server  *httptest.Server
	mu      sync.Mutex
	byTopic map[string][]CapturedProposal
	seen    map[string]map[uint64]bool // topic → raft index → already recorded
}

// collectBody mirrors the HTTP webhook syncable's payload: one committed
// Actual, its entities delivered together so the transaction boundary is
// intact (see internal/cluster/syncable/http).
type collectBody struct {
	Entities []collectEntity `json:"entities"`
}

type collectEntity struct {
	Key  string          `json:"key"` // base64-encoded entity key
	Type collectType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

type collectType struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

func newCollector() *collector {
	c := &collector{
		byTopic: make(map[string][]CapturedProposal),
		seen:    make(map[string]map[uint64]bool),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/collect/", c.handle)
	c.server = httptest.NewServer(mux)
	return c
}

func (c *collector) handle(w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimPrefix(r.URL.Path, "/collect/")
	// The Idempotency-Key is the Actual's raft index — its identity. We use
	// it to dedup a redelivery rather than for the HTTP idempotency a real
	// consumer would.
	idx, _ := strconv.ParseUint(r.Header.Get("Idempotency-Key"), 10, 64)

	var body collectBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cp := CapturedProposal{}
	for _, e := range body.Entities {
		// The webhook base64-encodes the entity key; decode it back so the
		// CapturedEntity matches what the oracle expects (the raw key).
		key, _ := base64.StdEncoding.DecodeString(e.Key)
		cp.Entities = append(cp.Entities, CapturedEntity{
			TypeID:   e.Type.ID,
			TypeName: e.Type.Name,
			Key:      string(key),
			Data:     string(e.Data),
		})
	}

	c.mu.Lock()
	if c.seen[topic] == nil {
		c.seen[topic] = make(map[uint64]bool)
	}
	if !c.seen[topic][idx] {
		c.seen[topic][idx] = true
		c.byTopic[topic] = append(c.byTopic[topic], cp)
	}
	c.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

// proposals returns a snapshot of the Actuals collected for one topic, in
// commit order. (The webhook delivers in raft-index order, so no reversal is
// needed — unlike the old reverse-log GET /proposal.)
func (c *collector) proposals(topic string) []CapturedProposal {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]CapturedProposal, len(c.byTopic[topic]))
	copy(out, c.byTopic[topic])
	return out
}

func (c *collector) urlFor(topic string) string {
	return c.server.URL + "/collect/" + topic
}

func (c *collector) close() {
	if c.server != nil {
		c.server.Close()
	}
}

// postCollectSyncable registers a webhook syncable that POSTs every committed
// Actual on `topic` to the harness collector. This is how the harness reads
// the committed stream — through a syncable, exactly as a real consumer
// would, since the log is write-only over HTTP.
func postCollectSyncable(t *testing.T, topic, url string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[syncable]\nname = %q\ntype = \"http\"\n\n", "collect-"+topic)
	fmt.Fprintf(&b, "[http]\ntopic = %q\nurl = %q\nmethod = \"POST\"\n", topic, url)
	postConfig(t, "/v1/syncable/collect_"+topic, b.String())
}
