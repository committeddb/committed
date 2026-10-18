package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// cluster talks to committed's HTTP API.
type cluster struct {
	nodes []string
	http  *http.Client
}

func newCluster(nodes []string) *cluster {
	return &cluster{nodes: nodes, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *cluster) do(ctx context.Context, method, url, body string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "text/toml")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// post posts a config to the first node that takes it.
func (c *cluster) post(ctx context.Context, path, body string) error {
	var last error
	for _, n := range c.nodes {
		code, b, err := c.do(ctx, http.MethodPost, n+path, body)
		if err == nil && code/100 == 2 {
			return nil
		}
		last = fmt.Errorf("POST %s%s: %d %s %v", n, path, code, strings.TrimSpace(string(b)), err)
	}
	return last
}

// delete deletes a config through the first node that takes it.
func (c *cluster) delete(ctx context.Context, path string) error {
	var last error
	for _, n := range c.nodes {
		code, b, err := c.do(ctx, http.MethodDelete, n+path, "")
		if err == nil && code/100 == 2 {
			return nil
		}
		last = fmt.Errorf("DELETE %s%s: %d %s %v", n, path, code, strings.TrimSpace(string(b)), err)
	}
	return last
}

// waitReady waits until every node answers /ready and they agree on a leader.
func (c *cluster) waitReady(ctx context.Context, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		ok, leader := true, uint64(0)
		for _, n := range c.nodes {
			code, _, err := c.do(ctx, http.MethodGet, n+"/ready", "")
			if err != nil || code != http.StatusOK {
				ok = false
				break
			}
			var st struct {
				Leader uint64 `json:"leader"`
			}
			if c.getJSON(ctx, n+"/v1/node/status", &st) != nil || st.Leader == 0 || (leader != 0 && st.Leader != leader) {
				ok = false
				break
			}
			leader = st.Leader
		}
		if ok {
			return nil
		}
		if err := sleep(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("the cluster was not ready within %s", limit)
}

// followerZone is the zone of a node that is not the leader: run.sh starts
// node i (in -nodes order, numbered from 1) in zone zi.
func (c *cluster) followerZone(ctx context.Context) (string, error) {
	var st struct {
		Leader uint64 `json:"leader"`
	}
	if err := c.getJSON(ctx, c.nodes[0]+"/v1/node/status", &st); err != nil {
		return "", err
	}
	for i := range c.nodes {
		if uint64(i+1) != st.Leader { //nolint:gosec // G115: i is a small index
			return fmt.Sprintf("z%d", i+1), nil
		}
	}
	return "", fmt.Errorf("no follower among %d nodes", len(c.nodes))
}

// waitApplySettled waits until every node reports the same applied index
// and it has not moved for three polls in a row — the log has stopped
// growing and every node has applied it. It returns that index.
func (c *cluster) waitApplySettled(ctx context.Context, limit time.Duration) (uint64, error) {
	deadline := time.Now().Add(limit)
	var last uint64
	steady := 0
	for time.Now().Before(deadline) {
		var lo, hi uint64
		ok := true
		for i, n := range c.nodes {
			var st struct {
				AppliedIndex uint64 `json:"appliedIndex"`
			}
			if c.getJSON(ctx, n+"/v1/node/status", &st) != nil {
				ok = false
				break
			}
			if i == 0 || st.AppliedIndex < lo {
				lo = st.AppliedIndex
			}
			hi = max(hi, st.AppliedIndex)
		}
		if ok && lo == hi && hi == last {
			if steady++; steady >= 3 {
				return hi, nil
			}
		} else {
			steady = 0
		}
		last = hi
		if err := sleep(ctx, 2*time.Second); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("the nodes' applied index did not settle within %s", limit)
}

// maxSyncLag is the largest lag any node reports for a syncable; known is
// false while no node reports one.
func (c *cluster) maxSyncLag(ctx context.Context, id string) (lag uint64, known bool) {
	for _, n := range c.nodes {
		var st syncStatus
		if c.getJSON(ctx, n+"/v1/syncable/"+id+"/status", &st) == nil && st.Lag != nil {
			lag, known = max(lag, *st.Lag), true
		}
	}
	return lag, known
}

// postRetrying posts a config, retrying for up to limit: a zone-pinned
// syncable is refused until the nodes have announced their zones.
func (c *cluster) postRetrying(ctx context.Context, path, body string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		err := c.post(ctx, path, body)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		if serr := sleep(ctx, time.Second); serr != nil {
			return serr
		}
	}
}

func (c *cluster) getJSON(ctx context.Context, url string, v any) error {
	code, b, err := c.do(ctx, http.MethodGet, url, "")
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("GET %s: %d", url, code)
	}
	return json.Unmarshal(b, v)
}

type ingestStatus struct {
	WorkerState string  `json:"workerState"`
	Phase       string  `json:"phase"`
	Lag         *uint64 `json:"lag"`
	LagUnit     string  `json:"lagUnit"`
	CaughtUp    bool    `json:"caughtUp"`
}

type syncStatus struct {
	Stuck       bool    `json:"stuck"`
	WorkerState string  `json:"workerState"`
	Lag         *uint64 `json:"lag"`
	CaughtUp    bool    `json:"caughtUp"`
	DeadLetters *int    `json:"deadLetters"`
}

// ingestable returns the status from the node running the worker: the one
// that reports a phase.
func (c *cluster) ingestable(ctx context.Context, id string) (ingestStatus, string) {
	var any ingestStatus
	for _, n := range c.nodes {
		var st ingestStatus
		if c.getJSON(ctx, n+"/v1/ingestable/"+id+"/status", &st) != nil {
			continue
		}
		if st.Phase != "" {
			return st, n
		}
		any = st
	}
	return any, ""
}

func (c *cluster) waitStreaming(ctx context.Context, id string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if st, _ := c.ingestable(ctx, id); st.Phase == "streaming" {
			return nil
		}
		if err := sleep(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("ingestable %s did not reach streaming within %s", id, limit)
}

// sampler records the cluster's view once a second while it runs.
type sampler struct {
	c       *cluster
	path    string
	start   time.Time
	appends func() int64
	pending func() int

	mu         sync.Mutex
	maxSyncLag uint64
}

type tick struct {
	T             float64            `json:"t"` // seconds since the run started
	Appended      int64              `json:"appended"`
	Unobserved    int                `json:"unobservedSamples"`
	AppliedIndex  map[string]uint64  `json:"appliedIndex"`
	Leader        uint64             `json:"leader"`
	IngestLag     *uint64            `json:"ingestLag"`
	IngestLagUnit string             `json:"ingestLagUnit,omitempty"`
	IngestNode    string             `json:"ingestNode,omitempty"`
	SyncLag       map[string]*uint64 `json:"syncLag"`
	Stuck         bool               `json:"stuck"`
	DeadLetters   int                `json:"deadLetters"`
}

func (s *sampler) run(ctx context.Context) {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // G304: results dir
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		t := tick{
			T: time.Since(s.start).Seconds(), Appended: s.appends(), Unobserved: s.pending(),
			AppliedIndex: map[string]uint64{}, SyncLag: map[string]*uint64{},
		}
		for _, n := range s.c.nodes {
			var ns struct {
				Leader       uint64 `json:"leader"`
				AppliedIndex uint64 `json:"appliedIndex"`
			}
			if s.c.getJSON(ctx, n+"/v1/node/status", &ns) == nil {
				t.AppliedIndex[n] = ns.AppliedIndex
				t.Leader = ns.Leader
			}
			var ss syncStatus
			if s.c.getJSON(ctx, n+"/v1/syncable/orders/status", &ss) == nil {
				t.SyncLag[n] = ss.Lag
				t.Stuck = t.Stuck || ss.Stuck
				if ss.DeadLetters != nil && *ss.DeadLetters > t.DeadLetters {
					t.DeadLetters = *ss.DeadLetters
				}
				if ss.Lag != nil {
					s.mu.Lock()
					s.maxSyncLag = max(s.maxSyncLag, *ss.Lag)
					s.mu.Unlock()
				}
			}
		}
		is, node := s.c.ingestable(ctx, "order-events")
		t.IngestLag, t.IngestLagUnit, t.IngestNode = is.Lag, is.LagUnit, node
		_ = enc.Encode(t)
	}
}

func (s *sampler) maxLag() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxSyncLag
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
