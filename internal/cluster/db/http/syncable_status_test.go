package http_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Status-endpoint tests against the real engine (see enginetest_test.go).
// Multi-node behaviors the old fake-driven tests simulated — the
// ?readPosition proxy hop to a remote owner, its soft degrade when the
// owner is unreachable, and the forwarded-request loop guard — are
// two-node semantics a single-node engine cannot induce and a fake can
// only pretend about; they belong in the multinode e2e harness (see the
// cluster-interface-retirement ticket). The build-degraded override is
// table-tested directly in syncable_mapping_test.go (applyBuildDegraded).

// TestSyncableStatus_HealthyShape: a fresh syncable with a drained log
// reports the healthy resting shape — running, not stuck, caught up, no
// dead letters, the single node as owner — and the opt-in fields stay
// absent without their params.
func TestSyncableStatus_HealthyShape(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")
	e.proposeRow(t, "photos", "k1")

	st := e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		caught, _ := m["caughtUp"].(bool)
		return caught
	}, "caught up after one row")
	require.Equal(t, false, st["stuck"])
	require.Equal(t, "running", st["workerState"])
	require.EqualValues(t, 0, st["lag"])
	require.EqualValues(t, 0, st["deadLetters"])
	require.EqualValues(t, 1, st["ownerNode"])
	require.NotContains(t, st, "readPosition",
		"the default call must not carry the position — opt-in only")
	require.NotContains(t, st, "stages", "stage counts are opt-in")
}

// TestSyncableStatus_LagDrains: a wedged sink shows positive lag and
// caughtUp=false; once the sink recovers the worker drains and the numbers
// return to rest. The full "X of Y, N behind" arithmetic incl. the clamp is
// pinned by TestProgressFields.
func TestSyncableStatus_LagDrains(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	e.sink.setErr(fmt.Errorf("hold the line"))
	e.proposeRow(t, "photos", "k1")
	e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		lag, _ := m["lag"].(float64)
		caught, _ := m["caughtUp"].(bool)
		return lag > 0 && !caught
	}, "positive lag while the sink refuses")

	e.sink.setErr(nil)
	e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		lag, _ := m["lag"].(float64)
		caught, _ := m["caughtUp"].(bool)
		return lag == 0 && caught
	}, "lag drains once the sink recovers")
	require.Equal(t, 1, e.sink.count())
}

// TestSyncableStatus_ReadPositionOptIn: ?readPosition=true on the owner
// (the single node) answers locally with the live scan position. A
// present-but-zero position is a real datum — "the worker has examined
// nothing yet", the exact split the phantom-adoption incident needed — so
// the pointer field must serialize 0 rather than omit it.
func TestSyncableStatus_ReadPositionOptIn(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	// Fresh worker: present and zero.
	require.Eventually(t, func() bool {
		w := e.doEmpty(t, "GET", "/v1/syncable/rec-1/status?readPosition=true")
		mustStatus(t, w, 200)
		var m map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
		_, present := m["readPosition"]
		return present
	}, 15*time.Second, 10*time.Millisecond,
		"the owner's live reader never surfaced a position")

	w := e.doEmpty(t, "GET", "/v1/syncable/rec-1/status?readPosition=true")
	mustStatus(t, w, 200)
	require.Contains(t, w.Body.String(), `"readPosition":`,
		"position must be present-and-numeric under the opt-in, even at 0")
}

// TestSyncableStatus_InvalidParams: malformed opt-in params are 400s —
// readPosition must be boolean, and probeStage/probeKey go together.
func TestSyncableStatus_InvalidParams(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/syncable/rec-1/status?readPosition=maybe"), 400, "invalid_parameter")
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/syncable/rec-1/status?probeStage=s1"), 400, "invalid_parameter")
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/syncable/rec-1/status?probeKey=k"), 400, "invalid_parameter")
}

// TestSyncableStatus_UnknownID404s: the existence gate — an unknown id's
// vacuous "running, caught up" must never render.
func TestSyncableStatus_UnknownID404s(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/syncable/nope/status"), 404, "not_found")
}

// TestAcknowledgeSyncableDeadLetter_NotDeadLettered: acknowledging an index
// that is not a dead letter 404s through the real engine. (The success path
// — acknowledge splits the counts and keeps the record listable — is part
// of TestSyncableDeadLetterJourney.)
func TestAcknowledgeSyncableDeadLetter_NotDeadLettered(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/syncable/rec-1/deadletter/99/acknowledge"), 404, "not_dead_lettered")
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/syncable/rec-1/deadletter/abc/acknowledge"), 400, "invalid_parameter")
}
