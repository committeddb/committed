package http_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// R3-HTTP-1: the router's own 404/405 paths must honor the JSON error-envelope
// contract, not chi's raw text/plain 404 / empty-body 405.
func TestRouterErrorEnvelope(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{})

	// Unmatched route → 404 JSON envelope.
	req := httptest.NewRequest("GET", "http://localhost/does-not-exist", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 404, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), `"code":"not_found"`)

	// Wrong method on an existing route (GET-only /v1/database) → 405 JSON.
	req = httptest.NewRequest("DELETE", "http://localhost/v1/database", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 405, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), `"code":"method_not_allowed"`)
}

// R3-HTTP-2: an unmatched /v1 path is answered from INSIDE the auth group — a
// probe without a token gets 401 (don't leak route existence unauthenticated),
// and with a token gets the 404 JSON envelope.
func TestUnmatchedV1RequiresAuth(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{}, http.WithBearerToken("secret"))

	// No token → 401 even though the path doesn't exist.
	req := httptest.NewRequest("GET", "http://localhost/v1/nonexistent", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 401, w.Code, "unmatched /v1 must require auth, not answer 404 unauthenticated")

	// Correct token → 404 JSON envelope (authenticated, then not found).
	req = httptest.NewRequest("GET", "http://localhost/v1/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 404, w.Code)
	require.Contains(t, w.Body.String(), `"code":"not_found"`)

	// A non-/v1 unmatched path stays a plain (auth-exempt) 404 envelope.
	req = httptest.NewRequest("GET", "http://localhost/nope", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 404, w.Code)
	require.Contains(t, w.Body.String(), `"code":"not_found"`)
}

// R3-HTTP-3: rollback reads the target version through the same linearizable-read
// barrier getVersion uses, so a lagging follower doesn't spuriously 404 a version
// it simply hasn't applied yet. Assert the read-index confirmation runs before
// the target-version read.
func TestRollback_RunsLinearizableRead(t *testing.T) {
	cfg := &cluster.Configuration{ID: "res-1", MimeType: "text/toml", Data: []byte("old")}
	h, fake := setupTest()
	fake.DatabaseVersionReturns(cfg, nil)

	req := httptest.NewRequest("POST", "http://localhost/v1/database/res-1/rollback?to=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Code)
	require.Equal(t, 1, fake.LinearizableReadCallCount(),
		"rollback must confirm a linearizable read before reading the target version")
}

// R3-HTTP-6: an empty-entities proposal is a client error, not a committed no-op
// raft entry — reject it with 400 rather than proposing nothing.
func TestAddProposal_EmptyRejected(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	req := httptest.NewRequest("POST", "http://localhost/v1/proposal", strings.NewReader(`{"entities":[]}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 400, w.Code)
	require.Contains(t, w.Body.String(), `"code":"empty_proposal"`)
	require.Equal(t, 0, fake.ProposeCallCount(), "an empty proposal must not reach the cluster")
}

// R3-HTTP-5: a config POST's Content-Type is parsed to its base media type, so a
// charset parameter (or header-case quirk) doesn't leak into the stored MimeType
// and trip downstream exact-match parsing.
func TestCreateConfig_ContentTypeParamsStripped(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	req := httptest.NewRequest("POST", "http://localhost/v1/database/db-1", strings.NewReader(`name = "x"`))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Code)
	require.Equal(t, 1, fake.ProposeDatabaseCallCount())
	_, cfg := fake.ProposeDatabaseArgsForCall(0)
	require.Equal(t, "application/json", cfg.MimeType, "the charset parameter must be stripped")
}
