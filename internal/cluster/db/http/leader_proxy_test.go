package http_test

import (
	"encoding/json"
	httpgo "net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

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
