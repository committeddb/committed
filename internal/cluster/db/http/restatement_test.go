package http_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

// The restatement surface against the real engine. Proposing is feature-
// gated (every member must announce restatement support), so the admitting
// tests run on an announcing engine and wait out the self-announcement the
// way a real rolling upgrade would; the dry-run deliberately works below
// the gate, and the below-gate refusal itself is a real 503 here.

const restatementTOML = "[restatement]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 5\nreadAsVersion = 2\n"

// seedRestatementTypes gives the restatement a real type with two versions
// to bind against.
func seedRestatementTypes(t *testing.T, e *engine) {
	t.Helper()
	e.addType(t, "photos", "photos")
	mustStatus(t, e.doTOML(t, "POST", "/v1/type/photos",
		"[type]\nname = \"photos\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n[migration]\nnone = true\n"), 200)
}

// awaitRestatementGate polls the propose path until the self-announcement
// applies and the feature gate opens (the same loop an operator's deploy
// tooling runs during a rolling upgrade).
func awaitRestatementGate(t *testing.T, e *engine) {
	t.Helper()
	require.Eventually(t, func() bool {
		w := e.doTOML(t, "POST", "/v1/restatement/gate-probe", restatementTOML)
		return w.Code == 200
	}, 15*time.Second, 10*time.Millisecond, "the restatement feature gate never opened")
}

// TestRestatementLifecycle: propose, immutability (an identical re-POST is
// an idempotent 200; different content is refused — corrections are NEW
// restatements), and the listing with the raft-index interpretation
// coordinate.
func TestRestatementLifecycle(t *testing.T) {
	e := newEngineOpts(t, db.WithVersionAnnounce())
	seedRestatementTypes(t, e)
	awaitRestatementGate(t, e)

	w := e.doTOML(t, "POST", "/v1/restatement/r1", restatementTOML)
	mustStatus(t, w, 200)
	require.Contains(t, w.Body.String(), `"id":"r1"`)

	// Idempotent identical re-POST.
	mustStatus(t, e.doTOML(t, "POST", "/v1/restatement/r1", restatementTOML), 200)

	// Immutable: different content under the same id is refused.
	edited := "[restatement]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 6\nreadAsVersion = 2\n"
	requireEnvelope(t, e.doTOML(t, "POST", "/v1/restatement/r1", edited), 400, "invalid_restatement_config")

	// The listing carries the registry in log order with raft indices.
	var listing []struct {
		ID            string `json:"id"`
		Type          string `json:"type"`
		FromIndex     uint64 `json:"fromIndex"`
		ToIndex       uint64 `json:"toIndex"`
		ReadAsVersion int    `json:"readAsVersion"`
		Index         uint64 `json:"index"`
	}
	w = e.doEmpty(t, "GET", "/v1/restatement")
	mustStatus(t, w, 200)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &listing))
	var r1 *struct {
		ID            string `json:"id"`
		Type          string `json:"type"`
		FromIndex     uint64 `json:"fromIndex"`
		ToIndex       uint64 `json:"toIndex"`
		ReadAsVersion int    `json:"readAsVersion"`
		Index         uint64 `json:"index"`
	}
	for i := range listing {
		if listing[i].ID == "r1" {
			r1 = &listing[i]
		}
	}
	require.NotNil(t, r1, "r1 must list")
	require.Equal(t, "photos", r1.Type)
	require.Equal(t, uint64(1), r1.FromIndex)
	require.Equal(t, uint64(5), r1.ToIndex)
	require.Equal(t, 2, r1.ReadAsVersion)
	require.NotZero(t, r1.Index, "the raft index is the interpretation coordinate")
}

// TestRestatement_BelowFeatureGateIs503: without announcements the cluster
// minimum stays 0 and a propose is refused retryably — the rolling-upgrade
// contract, through the real gate.
func TestRestatement_BelowFeatureGateIs503(t *testing.T) {
	e := newEngine(t) // no announce
	seedRestatementTypes(t, e)
	requireEnvelope(t, e.doTOML(t, "POST", "/v1/restatement/r1", restatementTOML),
		503, "cluster_below_feature_level")
}

// TestDryRunRestatement: the rehearsal runs the real admission words and the
// real fold — and deliberately works below the feature gate (a finding, not
// a refusal), because the dry-run IS the authoring loop.
func TestDryRunRestatement(t *testing.T) {
	e := newEngine(t) // no announce: below the gate on purpose
	seedRestatementTypes(t, e)
	e.addRecorderSyncable(t, "rec-1", "photos")
	e.proposeRow(t, "photos", "k1")

	// The window runs to the LIVE applied index (admission refuses anything
	// beyond the applied log — a restatement rebinds existing actuals), read
	// from the node's own status.
	var st struct {
		AppliedIndex uint64 `json:"appliedIndex"`
	}
	e.getJSON(t, "/v1/node/status", &st)
	require.NotZero(t, st.AppliedIndex)
	wide := fmt.Sprintf("[restatement]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = %d\nreadAsVersion = 2\n", st.AppliedIndex)
	w := e.doTOML(t, "POST", "/v1/restatement/dryrun", wide)
	mustStatus(t, w, 200)
	var rep struct {
		EntriesScanned int      `json:"entriesScanned"`
		EntitiesOfType int      `json:"entitiesOfType"`
		Findings       []string `json:"findings"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rep))
	require.NotZero(t, rep.EntriesScanned, "the rehearsal scans the real log")
	require.Equal(t, 1, rep.EntitiesOfType, "the census sees the real proposed row")

	// A config the admission path rejects comes back with its actual words.
	bad := "[restatement]\ntype = \"no-such-type\"\nfromIndex = 1\ntoIndex = 5\nreadAsVersion = 2\n"
	requireEnvelope(t, e.doTOML(t, "POST", "/v1/restatement/dryrun", bad), 400, "invalid_config")
}
