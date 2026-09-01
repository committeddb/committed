package http_test

import (
	"encoding/json"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

// doMembershipRequest performs GET /v1/membership against h with optional
// extra headers, returning the status and body.
func doMembershipRequest(t *testing.T, h *http.HTTP, headers map[string]string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(httpgo.MethodGet, "http://localhost/v1/membership", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code, w.Body.Bytes()
}

// TestMembership_ServedLocallyOnLeader: when this node is the leader, the
// GET is answered from its own live Membership() — no proxying — and the
// liveness field serializes honestly: the ghost learner (added but never
// started) must read an EXPLICIT "active":false, never be silently dropped
// by omitempty. matchIndex (replication memory) rides next to it.
func TestMembership_ServedLocallyOnLeader(t *testing.T) {
	e := newEngine(t)
	mustStatus(t, e.doJSON(t, "POST", "/v1/membership", `{"id":2,"url":"http://127.0.0.1:9999","learner":true}`), 204)

	require.Eventually(t, func() bool {
		w := e.doEmpty(t, "GET", "/v1/membership")
		if w.Code != httpgo.StatusOK {
			return false
		}
		var resp http.MembershipResponse
		if json.Unmarshal(w.Body.Bytes(), &resp) != nil {
			return false
		}
		if !resp.IsLeader || len(resp.Members) != 2 {
			return false
		}
		var ghost *http.MemberResponse
		for i := range resp.Members {
			if resp.Members[i].ID == 2 {
				ghost = &resp.Members[i]
			}
		}
		// The leader's liveness sweep needs an election timeout to mark the
		// never-started learner inactive.
		if ghost == nil || ghost.Active == nil || *ghost.Active {
			return false
		}
		require.Equal(t, cluster.MemberRoleLearner, ghost.Role)
		require.Contains(t, w.Body.String(), `"active":false`,
			"an inactive member must be explicitly false in the JSON, not omitted")
		require.NotZero(t, resp.CommitIndex)
		return true
	}, 15*time.Second, 10*time.Millisecond, "the ghost learner never read active:false on the live listing")
}

// TestMembership_ProxiedFromFollower: a follower forwards the request to the
// leader's announced API URL and returns the leader's response verbatim,
// carrying the loop-guard marker and forwarding the Authorization header.
func TestMembership_ProxiedFromFollower(t *testing.T) {
	const leaderBody = `{"nodeId":1,"leaderId":1,"isLeader":true,"members":[{"id":1,"role":"voter","matchIndex":99}]}`

	var (
		mu       sync.Mutex
		gotAuth  string
		gotFwd   string
		gotPath  string
		gotCalls int
	)
	leader := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotFwd = r.Header.Get("X-Committed-Forwarded")
		gotPath = r.URL.Path
		gotCalls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, leaderBody)
	}))
	defer leader.Close()

	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)     // this node
	fake.LeaderReturns(1) // leader is node 1
	fake.MemberAPIURLReturns(leader.URL, true)
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, map[string]string{"Authorization": "Bearer t0ken"})
	require.Equal(t, httpgo.StatusOK, status)
	require.JSONEq(t, leaderBody, string(body))

	// The follower proxied rather than answering locally: the fake has no
	// engine behind it, so a local answer would have been a 500, and the
	// verbatim leader body above proves the hop.
	id := fake.MemberAPIURLArgsForCall(0)
	require.Equal(t, uint64(1), id)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, gotCalls)
	require.Equal(t, "/v1/membership", gotPath)
	require.Equal(t, "Bearer t0ken", gotAuth, "Authorization must chain to the leader")
	require.Equal(t, "1", gotFwd, "loop-guard marker must be set on the forwarded request")
}

// TestMembership_LeaderUnreachable: when the leader's announced address can't
// be dialed, the follower returns 503 rather than hanging.
func TestMembership_LeaderUnreachable(t *testing.T) {
	// Start then immediately stop a server so its address is guaranteed to
	// refuse connections.
	dead := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {}))
	deadURL := dead.URL
	dead.Close()

	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(1)
	fake.MemberAPIURLReturns(deadURL, true)
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, nil)
	require.Equal(t, httpgo.StatusServiceUnavailable, status)
	require.Equal(t, "leader_unavailable", errorCode(t, body))
}

// TestMembership_ProxiedOverTLS: the follower→leader hop works against a
// TLS-serving leader when the proxy client is configured to trust it
// (WithProxyClient), exercising the TLS path cmd/node.go wires up.
func TestMembership_ProxiedOverTLS(t *testing.T) {
	const leaderBody = `{"nodeId":1,"leaderId":1,"isLeader":true,"members":[]}`
	leader := httptest.NewTLSServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, leaderBody)
	}))
	defer leader.Close()

	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(1)
	fake.MemberAPIURLReturns(leader.URL, true)
	// leader.Client() trusts the test server's self-signed cert.
	h := http.New(fake, http.WithProxyClient(leader.Client()))

	status, body := doMembershipRequest(t, h, nil)
	require.Equal(t, httpgo.StatusOK, status)
	require.JSONEq(t, leaderBody, string(body))
}

// TestMembership_LoopGuard: a request that already carries the forwarded
// marker but lands on a non-leader returns 503 instead of forwarding again.
func TestMembership_LoopGuard(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(1)
	fake.MemberAPIURLReturns("http://n1:8080", true)
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, map[string]string{"X-Committed-Forwarded": "1"})
	require.Equal(t, httpgo.StatusServiceUnavailable, status)
	require.Equal(t, "leader_unavailable", errorCode(t, body))
	// Must not have attempted to resolve a URL / forward again.
	require.Equal(t, 0, fake.MemberAPIURLCallCount())
}

// TestMembership_DegradedLeaderHasNoAPIURL: a follower whose leader never
// announced an API URL returns 503 with the believed leader id, so the caller
// can target the leader directly.
func TestMembership_DegradedLeaderHasNoAPIURL(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(1)
	fake.MemberAPIURLReturns("", false)
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, nil)
	require.Equal(t, httpgo.StatusServiceUnavailable, status)
	require.Equal(t, "leader_unavailable", errorCode(t, body))
	require.Equal(t, float64(1), leaderIDDetail(t, body))
}

// TestMembership_NoLeaderKnown: with no leader elected the read is unavailable.
func TestMembership_NoLeaderKnown(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(0)
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, nil)
	require.Equal(t, httpgo.StatusServiceUnavailable, status)
	require.Equal(t, "leader_unavailable", errorCode(t, body))
	require.Equal(t, float64(0), leaderIDDetail(t, body))
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	var e struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(body, &e))
	return e.Code
}

func leaderIDDetail(t *testing.T, body []byte) float64 {
	t.Helper()
	var e struct {
		Details struct {
			LeaderID float64 `json:"leaderId"`
		} `json:"details"`
	}
	require.NoError(t, json.Unmarshal(body, &e))
	return e.Details.LeaderID
}
