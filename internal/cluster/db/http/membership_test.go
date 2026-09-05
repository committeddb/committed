package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Membership changes against the real engine. A single-node cluster can add
// and remove a LEARNER for real (learners never count toward quorum, so the
// engine stays live), refuse to remove its last voter, and refuse to promote
// a non-learner — the joint-consensus voter journeys (grow, promote) run as
// real multi-process clusters in the multinode e2e suite. Error legs the
// engine cannot produce here (unconfirmed 503) are pinned in
// membership_mapping_test.go.

type membershipView struct {
	NodeID   uint64 `json:"nodeId"`
	LeaderID uint64 `json:"leaderId"`
	IsLeader bool   `json:"isLeader"`
	Members  []struct {
		ID   uint64 `json:"id"`
		Role string `json:"role"`
	} `json:"members"`
}

func getMembership(t *testing.T, e *engine) membershipView {
	t.Helper()
	w := e.doEmpty(t, "GET", "/v1/membership")
	require.Equal(t, 200, w.Code, w.Body.String())
	var m membershipView
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
	return m
}

// TestMembership_LearnerLifecycle: add a learner (204), see it listed with
// its role, remove it (204), see it gone — real joint-consensus conf
// changes end to end on the live node.
func TestMembership_LearnerLifecycle(t *testing.T) {
	e := newEngine(t)

	m := getMembership(t, e)
	require.Equal(t, uint64(1), m.NodeID)
	require.Equal(t, uint64(1), m.LeaderID)
	require.True(t, m.IsLeader)
	require.Len(t, m.Members, 1)
	require.Equal(t, "voter", m.Members[0].Role)

	w := e.doJSON(t, "POST", "/v1/membership", `{"id":2,"url":"http://127.0.0.1:9999","learner":true}`)
	require.Equal(t, 204, w.Code, w.Body.String())

	m = getMembership(t, e)
	require.Len(t, m.Members, 2)
	roles := map[uint64]string{}
	for _, mem := range m.Members {
		roles[mem.ID] = mem.Role
	}
	require.Equal(t, "voter", roles[1])
	require.Equal(t, "learner", roles[2])

	w = e.doEmpty(t, "DELETE", "/v1/membership/2")
	require.Equal(t, 204, w.Code, w.Body.String())
	m = getMembership(t, e)
	require.Len(t, m.Members, 1)
}

// TestRemoveMember_LastVoter: the engine's own guard — removing the sole
// voter is refused 409 (well-formed, conflicts with the cluster's state).
func TestRemoveMember_LastVoter(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "DELETE", "/v1/membership/1"), 409, "would_remove_last_voter")
}

// TestPromoteMember_NotLearner: promoting a voter (or unknown id) is a real
// 400 from the engine's learner check.
func TestPromoteMember_NotLearner(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/membership/1/promote"), 400, "not_a_learner")
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/membership/99/promote"), 400, "not_a_learner")
}

// TestMembership_BadRequests: malformed JSON and malformed ids never reach
// a conf change.
func TestMembership_BadRequests(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doJSON(t, "POST", "/v1/membership", `{not json`), 400, "invalid_json")
	requireEnvelope(t, e.doEmpty(t, "DELETE", "/v1/membership/abc"), 400, "invalid_member_id")
	requireEnvelope(t, e.doEmpty(t, "DELETE", "/v1/membership/0"), 400, "invalid_member_id")
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/membership/abc/promote"), 400, "invalid_member_id")
}
