package http_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

func doRequest(t *testing.T, h *http.HTTP, method, target, body string) int {
	t.Helper()
	r := httptest.NewRecorder()
	reqBody := strings.NewReader(body)
	req := httptest.NewRequest(method, target, reqBody)
	h.ServeHTTP(r, req)
	return r.Result().StatusCode
}

// TestAddMember_Success verifies POST /v1/membership forwards the parsed id
// and url to Cluster.AddMember and returns 204 on success.
func TestAddMember_Success(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/membership",
		`{"id": 4, "url": "http://127.0.0.1:9024"}`)

	require.Equal(t, 204, status)
	require.Equal(t, 1, fake.AddMemberCallCount())
	_, id, url := fake.AddMemberArgsForCall(0)
	require.Equal(t, uint64(4), id)
	require.Equal(t, "http://127.0.0.1:9024", url)
}

// TestAddMember_InvalidJSON returns 400 without touching the cluster.
func TestAddMember_InvalidJSON(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/membership", `{not json`)

	require.Equal(t, 400, status)
	require.Equal(t, 0, fake.AddMemberCallCount())
}

// TestAddMember_InvalidMember maps cluster.ErrInvalidMember to 400.
func TestAddMember_InvalidMember(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.AddMemberReturns(cluster.ErrInvalidMember)
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/membership",
		`{"id": 0, "url": ""}`)

	require.Equal(t, 400, status)
}

// TestAddMember_Unconfirmed maps a context error (couldn't confirm the change
// committed before the deadline) to 503.
func TestAddMember_Unconfirmed(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.AddMemberReturns(context.DeadlineExceeded)
	h := http.New(fake)

	status := doRequest(t, h, "POST", "http://localhost/v1/membership",
		`{"id": 4, "url": "http://127.0.0.1:9024"}`)

	require.Equal(t, 503, status)
}

// TestRemoveMember_Success verifies DELETE /v1/membership/{id} forwards the
// parsed id to Cluster.RemoveMember and returns 204.
func TestRemoveMember_Success(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status := doRequest(t, h, "DELETE", "http://localhost/v1/membership/3", "")

	require.Equal(t, 204, status)
	require.Equal(t, 1, fake.RemoveMemberCallCount())
	_, id := fake.RemoveMemberArgsForCall(0)
	require.Equal(t, uint64(3), id)
}

// TestRemoveMember_BadID rejects a non-numeric id with 400 and never calls
// the cluster.
func TestRemoveMember_BadID(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status := doRequest(t, h, "DELETE", "http://localhost/v1/membership/abc", "")

	require.Equal(t, 400, status)
	require.Equal(t, 0, fake.RemoveMemberCallCount())
}

// TestRemoveMember_ZeroID rejects id 0 (the reserved "no node" value) with
// 400.
func TestRemoveMember_ZeroID(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status := doRequest(t, h, "DELETE", "http://localhost/v1/membership/0", "")

	require.Equal(t, 400, status)
	require.Equal(t, 0, fake.RemoveMemberCallCount())
}
