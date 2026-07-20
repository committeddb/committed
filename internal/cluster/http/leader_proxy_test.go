package http_test

import (
	"encoding/json"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

func u64p(v uint64) *uint64 { return &v }

// doMembershipRequest issues GET /v1/membership against h with optional extra
// headers and returns the status, parsed error body (if any), and raw body.
func doMembershipRequest(t *testing.T, h *http.HTTP, headers map[string]string) (int, []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(httpgo.MethodGet, "http://lb.example/v1/membership", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	h.ServeHTTP(rec, req)
	res := rec.Result()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, body
}

// TestMembership_ServedLocallyOnLeader: when this node is the leader, the GET
// is answered from its own Membership() — no proxying.
func TestMembership_ServedLocallyOnLeader(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(1)
	fake.LeaderReturns(1)
	fake.MembershipReturns(cluster.Membership{
		NodeID: 1, LeaderID: 1, Term: 3, CommitIndex: 42, AppliedIndex: 42, IsLeader: true,
		Members: []cluster.Member{
			{ID: 1, Role: cluster.MemberRoleVoter, MatchIndex: u64p(42), APIURL: "http://n1:8080"},
			{ID: 2, Role: cluster.MemberRoleVoter, MatchIndex: u64p(40), APIURL: "http://n2:8080"},
		},
	})
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, nil)
	require.Equal(t, httpgo.StatusOK, status)

	var resp http.MembershipResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Equal(t, uint64(1), resp.NodeID)
	require.True(t, resp.IsLeader)
	require.Equal(t, uint64(42), resp.CommitIndex)
	require.Len(t, resp.Members, 2)
	require.Equal(t, uint64(1), resp.Members[0].ID)
	require.Equal(t, cluster.MemberRoleVoter, resp.Members[0].Role)
	require.NotNil(t, resp.Members[0].MatchIndex)
	require.Equal(t, uint64(42), *resp.Members[0].MatchIndex)
	require.Equal(t, "http://n2:8080", resp.Members[1].APIURL)
}

// TestMembership_ReportsLearnerRole verifies GET /v1/membership distinguishes
// voters from learners in its response (each carrying its match index).
func TestMembership_ReportsLearnerRole(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(1)
	fake.LeaderReturns(1)
	fake.MembershipReturns(cluster.Membership{
		NodeID: 1, LeaderID: 1, IsLeader: true, CommitIndex: 100,
		Members: []cluster.Member{
			{ID: 1, Role: cluster.MemberRoleVoter, MatchIndex: u64p(100)},
			{ID: 4, Role: cluster.MemberRoleLearner, MatchIndex: u64p(80)},
		},
	})
	h := http.New(fake)

	status, body := doMembershipRequest(t, h, nil)
	require.Equal(t, httpgo.StatusOK, status)

	var resp http.MembershipResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	roles := map[uint64]string{}
	for _, m := range resp.Members {
		roles[m.ID] = m.Role
	}
	require.Equal(t, cluster.MemberRoleVoter, roles[1])
	require.Equal(t, cluster.MemberRoleLearner, roles[4])
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

	// The follower proxied rather than answering locally.
	require.Equal(t, 0, fake.MembershipCallCount())
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
