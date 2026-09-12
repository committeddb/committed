//go:build multinode

package multinode_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMultiNodeSyncableOwnerProxy covers the syncable owner-proxy http
// behaviors that only a real second node can exercise truthfully (the
// cluster.Cluster retirement converted the http suite to live single-node
// engines, where these hops have no counterparty — see
// tickets/multinode-http-proxy-coverage.md):
//
//  1. GET /syncable/{id}/status?readPosition=true on a NON-owner proxies the
//     whole request to the pinned owner's announced API URL — the scan
//     position lives only in the owner's worker, so its presence in the
//     response IS the proof of the hop.
//  2. rebuild POSTed at a non-owner forwards to the pinned owner and runs
//     there (db.RebuildSyncable refuses on a non-owner, so a 202 can only
//     come from the owner); a request already carrying the forwarded marker
//     is refused (503 not_syncable_owner), never re-forwarded.
//  3. With the owner dead, the status read SOFT-DEGRADES: 200 with every
//     replicated field and ownerNode naming who to ask, readPosition absent
//     — not a 503.
//
// The cluster is 3 nodes, not the ticket's original 2: the degraded phase
// kills the owner, and the surviving nodes must still hold quorum or the
// consistency barrier (not the degrade path) answers with its own 503.
// Node 1 is alone in zone-a and owns the pinned syncable; nodes 2 and 3 sit
// in zone-b and survive it.
func TestMultiNodeSyncableOwnerProxy(t *testing.T) {
	buildBinary(t)
	nodes := startClusterWithZones(t, "zone-a", "zone-b", "zone-b")
	owner, nonOwner := nodes[0], nodes[1]

	// The webhook sink the syncable syncs into, hosted by the test — the
	// only destination kind a real binary can serve without external infra.
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	postType(t, nonOwner.base(), "wh-topic")

	// Zone-pin admission opens only after every member's feature-level
	// announcement AND node 1's zone announcement commit (both async
	// post-boot proposals), so the POST polls until admitted.
	syncableTOML := fmt.Sprintf(
		"[syncable]\nname = \"wh\"\ntype = \"http\"\nzone = \"zone-a\"\n\n[http]\ntopic = \"wh-topic\"\nurl = %q\nmethod = \"POST\"\ntimeoutMs = 3000\n",
		sink.URL)
	postSyncableUntilAdmitted(t, nonOwner.base(), "wh", syncableTOML)

	// Data in the topic, so the owner's worker reads and holds a live scan
	// position.
	postProposal(t, nonOwner.base(), "wh-topic", "k-1")

	// Phase 1: the non-owner answer carries the owner-local readPosition —
	// only the proxy hop to node 1 can produce it (B has no worker). Polled:
	// the owner's worker publishes its reader some moments after admission.
	st := awaitReadPosition(t, nonOwner.base(), "wh")
	require.Equal(t, owner.id, st.OwnerNode, "ownerNode must name the pinned owner")

	// Phase 1b: the same request with the loop-guard marker pre-set answers
	// LOCALLY — 200 with the replicated fields, readPosition absent —
	// proving the field is owner-local and phase 1's copy came off the hop,
	// and that a forwarded request is never forwarded again.
	code, st2, body := syncableStatus(t, nonOwner.base(), "wh", map[string]string{forwardedMarkerHeader: "1"})
	require.Equal(t, http.StatusOK, code, "marked request must serve locally, got: %s", body)
	require.Nil(t, st2.ReadPosition, "a non-owner answering locally must not synthesize readPosition")
	require.Equal(t, owner.id, st2.OwnerNode)

	// Phase 2: rebuild through the non-owner reaches the owner and runs
	// there. db.RebuildSyncable refuses off-owner (not_syncable_owner), so
	// 202 proves owner execution. Polled: the hop rides a bounded proxy
	// timeout and the verb takes two consensus round-trips.
	postRebuildUntil(t, nonOwner.base(), "wh", http.StatusAccepted)

	// Phase 2b: the forwarded marker pre-set on a non-owner is refused
	// deterministically — "ownership moved while routing" — never
	// re-forwarded.
	code, rebuildBody := postRebuildOnce(t, nonOwner.base(), "wh", map[string]string{forwardedMarkerHeader: "1"})
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Contains(t, rebuildBody, "not_syncable_owner")

	// Phase 3: kill the owner (dirty). Quorum survives on zone-b; ownership
	// still resolves to the dead node 1 (membership and its announced zone
	// are replicated state, not liveness). The status read must soft-degrade
	// — 200, replicated fields, no readPosition, ownerNode saying who to
	// ask — not a 503. Polled: if node 1 held raft leadership, the
	// consistency barrier 503s until the survivors elect.
	owner.kill(t)
	deadline := time.Now().Add(60 * time.Second)
	for {
		code, st, body = syncableStatusFull(t, nonOwner.base(), "wh", nil)
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status did not soft-degrade to 200 after the owner died: %d %s", code, body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.Nil(t, st.ReadPosition, "readPosition must be absent when the owner is unreachable")
	require.Equal(t, owner.id, st.OwnerNode, "the degraded answer must still name the owner to ask")
}

// forwardedMarkerHeader is the proxy loop-guard header
// (internal/cluster/db/http leader_proxy). Spelled out here because the e2e
// suite speaks to the binary only over the wire.
const forwardedMarkerHeader = "X-Committed-Forwarded"

// syncableStatusView is the slice of the status response this test asserts.
type syncableStatusView struct {
	OwnerNode    uint64  `json:"ownerNode"`
	ReadPosition *uint64 `json:"readPosition"`
}

// startClusterWithZones boots one node per zone label (ids 1..n) with
// COMMITTED_ZONE set, sharing one COMMITTED_PEERS, and waits for /ready —
// startCluster plus placement identity.
func startClusterWithZones(t *testing.T, zones ...string) []*clusterNode {
	t.Helper()
	nodes := make([]*clusterNode, len(zones))
	peerPairs := make([]string, len(zones))
	for i := range nodes {
		nodes[i] = &clusterNode{
			id: uint64(i + 1), dataDir: t.TempDir(),
			apiPort: freePort(t), raftPort: freePort(t),
		}
		peerPairs[i] = fmt.Sprintf("%d=http://127.0.0.1:%d", i+1, nodes[i].raftPort)
	}
	peers := strings.Join(peerPairs, ",")
	for i, n := range nodes {
		n.env = append(nodeEnv(n.id, n.dataDir, n.apiPort, peers, false),
			"COMMITTED_ZONE="+zones[i])
		n.start(t)
		t.Cleanup(func() { n.kill(t) })
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	return nodes
}

// postSyncableUntilAdmitted POSTs the syncable config until the cluster
// admits it. A zone-pinned config is refused until the feature-level and
// zone announcements of every member commit, so early refusals are the
// expected path, not failures.
func postSyncableUntilAdmitted(t *testing.T, base, id, toml string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastCode int
	var lastBody string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/syncable/"+id, strings.NewReader(toml))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "text/toml")
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			out, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			lastCode, lastBody = resp.StatusCode, string(out)
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("syncable %q was never admitted: last %d %s", id, lastCode, lastBody)
}

// syncableStatus GETs /v1/syncable/{id}/status?readPosition=true with the
// given extra headers and decodes the ownerNode/readPosition slice.
func syncableStatus(t *testing.T, base, id string, headers map[string]string) (int, syncableStatusView, string) {
	t.Helper()
	return syncableStatusFull(t, base, id, headers)
}

func syncableStatusFull(t *testing.T, base, id string, headers map[string]string) (int, syncableStatusView, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/syncable/"+id+"/status?readPosition=true", nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, syncableStatusView{}, err.Error()
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	var st syncableStatusView
	_ = json.Unmarshal(out, &st)
	return resp.StatusCode, st, string(out)
}

// awaitReadPosition polls the status through base until the response carries
// readPosition — the owner's worker publishes its reader shortly after the
// config applies.
func awaitReadPosition(t *testing.T, base, id string) syncableStatusView {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastCode int
	var lastBody string
	for time.Now().Before(deadline) {
		code, st, body := syncableStatusFull(t, base, id, nil)
		if code == http.StatusOK && st.ReadPosition != nil {
			return st
		}
		lastCode, lastBody = code, body
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("readPosition never appeared via %s: last %d %s", base, lastCode, lastBody)
	return syncableStatusView{}
}

// postRebuildOnce POSTs /v1/syncable/{id}/rebuild once.
func postRebuildOnce(t *testing.T, base, id string, headers map[string]string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/syncable/"+id+"/rebuild", nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

// postRebuildUntil retries the rebuild until it returns want. Rebuild is
// re-runnable by construction (idempotent teardown + replay from 0), so
// retrying through transient hop timeouts is safe; a real routing regression
// (a non-owner refusing locally) is a STABLE non-202 and fails the deadline
// with the last response shown.
func postRebuildUntil(t *testing.T, base, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastCode int
	var lastBody string
	for time.Now().Before(deadline) {
		lastCode, lastBody = postRebuildOnce(t, base, id, nil)
		if lastCode == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("rebuild via %s never returned %d: last %d %s", base, want, lastCode, lastBody)
}
