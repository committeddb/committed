package http

import (
	"encoding/json"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The leaderRead proxy hop, tested as the middleware unit it is, over the
// package-owned clusterView stub — the follower legs (id != leader) are
// two-node topology a single live engine cannot produce; the real voter
// journeys run in the multinode e2e suite, and the local-serve leg runs
// real in leader_proxy_test.go.

func proxyHTTP(view *viewStub, client *httpgo.Client) *HTTP {
	if client == nil {
		client = &httpgo.Client{Timeout: defaultProxyTimeout}
	}
	return &HTTP{view: view, proxyClient: client}
}

// marker is a next-handler that must never run on a proxied request.
func marker(ran *bool) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) { *ran = true }
}

func doLeaderRead(h *HTTP, headers map[string]string, ran *bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(httpgo.MethodGet, "http://localhost/v1/membership", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.leaderRead(marker(ran))(w, req)
	return w
}

// TestLeaderRead_ProxiedFromFollower: a follower forwards to the leader's
// announced API URL and returns the leader's response verbatim, carrying
// the loop-guard marker and forwarding the Authorization header; the local
// handler never runs.
func TestLeaderRead_ProxiedFromFollower(t *testing.T) {
	const leaderBody = `{"nodeId":1,"leaderId":1,"isLeader":true,"members":[{"id":1,"role":"voter","matchIndex":99}]}`
	var (
		mu      sync.Mutex
		gotAuth string
		gotFwd  string
		gotPath string
	)
	leader := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotFwd = r.Header.Get("X-Committed-Forwarded")
		gotPath = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, leaderBody)
	}))
	defer leader.Close()

	view := &viewStub{id: 2, leader: 1, apiURLs: map[uint64]string{1: leader.URL}}
	ran := false
	w := doLeaderRead(proxyHTTP(view, nil), map[string]string{"Authorization": "Bearer t0ken"}, &ran)

	require.Equal(t, httpgo.StatusOK, w.Code)
	require.JSONEq(t, leaderBody, w.Body.String())
	require.False(t, ran, "the follower must proxy, never answer locally")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "/v1/membership", gotPath)
	require.Equal(t, "Bearer t0ken", gotAuth, "Authorization must chain to the leader")
	require.Equal(t, "1", gotFwd, "loop-guard marker must be set on the forwarded request")
}

// TestLeaderRead_LeaderUnreachable: when the leader's announced address
// can't be dialed, the follower returns 503 rather than hanging.
func TestLeaderRead_LeaderUnreachable(t *testing.T) {
	dead := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {}))
	deadURL := dead.URL
	dead.Close()

	view := &viewStub{id: 2, leader: 1, apiURLs: map[uint64]string{1: deadURL}}
	ran := false
	w := doLeaderRead(proxyHTTP(view, nil), nil, &ran)
	require.Equal(t, httpgo.StatusServiceUnavailable, w.Code)
	require.Equal(t, "leader_unavailable", unitErrorCode(t, w.Body.Bytes()))
	require.False(t, ran)
}

// TestLeaderRead_ProxiedOverTLS: the hop works against a TLS-serving leader
// when the proxy client trusts it (WithProxyClient — the path cmd/node.go
// wires up with the cluster CA).
func TestLeaderRead_ProxiedOverTLS(t *testing.T) {
	const leaderBody = `{"nodeId":1,"leaderId":1,"isLeader":true,"members":[]}`
	leader := httptest.NewTLSServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, leaderBody)
	}))
	defer leader.Close()

	view := &viewStub{id: 2, leader: 1, apiURLs: map[uint64]string{1: leader.URL}}
	ran := false
	w := doLeaderRead(proxyHTTP(view, leader.Client()), nil, &ran)
	require.Equal(t, httpgo.StatusOK, w.Code)
	require.JSONEq(t, leaderBody, w.Body.String())
}

// TestLeaderRead_LoopGuard: a request already carrying the forwarded marker
// that lands on a non-leader returns 503 instead of forwarding again.
func TestLeaderRead_LoopGuard(t *testing.T) {
	view := &viewStub{id: 2, leader: 1, apiURLs: map[uint64]string{1: "http://n1:8080"}}
	ran := false
	w := doLeaderRead(proxyHTTP(view, nil), map[string]string{"X-Committed-Forwarded": "1"}, &ran)
	require.Equal(t, httpgo.StatusServiceUnavailable, w.Code)
	require.Equal(t, "leader_unavailable", unitErrorCode(t, w.Body.Bytes()))
	require.Zero(t, view.memberAPIURLCalls, "must not resolve a URL / forward again")
}

// TestLeaderRead_DegradedLeaderHasNoAPIURL: a follower whose leader never
// announced an API URL returns 503 with the believed leader id, so the
// caller can target the leader directly.
func TestLeaderRead_DegradedLeaderHasNoAPIURL(t *testing.T) {
	view := &viewStub{id: 2, leader: 1}
	ran := false
	w := doLeaderRead(proxyHTTP(view, nil), nil, &ran)
	require.Equal(t, httpgo.StatusServiceUnavailable, w.Code)
	require.Equal(t, "leader_unavailable", unitErrorCode(t, w.Body.Bytes()))
	require.Equal(t, float64(1), unitLeaderIDDetail(t, w.Body.Bytes()))
}

// TestLeaderRead_NoLeaderKnown: with no leader elected the read is
// unavailable.
func TestLeaderRead_NoLeaderKnown(t *testing.T) {
	view := &viewStub{id: 2, leader: 0}
	ran := false
	w := doLeaderRead(proxyHTTP(view, nil), nil, &ran)
	require.Equal(t, httpgo.StatusServiceUnavailable, w.Code)
	require.Equal(t, "leader_unavailable", unitErrorCode(t, w.Body.Bytes()))
	require.Equal(t, float64(0), unitLeaderIDDetail(t, w.Body.Bytes()))
}

func unitErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var e struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(body, &e))
	return e.Code
}

func unitLeaderIDDetail(t *testing.T, body []byte) float64 {
	t.Helper()
	var e struct {
		Details struct {
			LeaderID float64 `json:"leaderId"`
		} `json:"details"`
	}
	require.NoError(t, json.Unmarshal(body, &e))
	return e.Details.LeaderID
}
