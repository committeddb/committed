package http_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/http"
)

// The syncable handler group holds the engine (*db.DB) concretely, so these
// tests run against a real single-node engine (see enginetest_test.go) and
// induce states through the plugin seam — a recorder sink that fails on
// command — instead of stubbing engine answers. Error mappings that cannot
// be induced deterministically (a wedged worker, a mid-flap ownership move)
// are covered by the mapping-table tests in syncable_mapping_test.go.

// TestSyncableLifecycle_ProposeListVersionsRollback drives the config CRUD
// surface end to end: two versions, the listing, version reads, and a
// rollback that creates version 3 with version 1's content.
func TestSyncableLifecycle_ProposeListVersionsRollback(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")

	v1 := "[syncable]\nname = \"rec-1\"\ntype = \"recorder\"\n[recorder]\ntopic = \"photos\"\n"
	v2 := "[syncable]\nname = \"rec-1\"\ntype = \"recorder\"\n[recorder]\ntopic = \"photos\"\nnote = \"v2\"\n"

	w := e.doTOML(t, "POST", "/v1/syncable/rec-1", v1)
	mustStatus(t, w, 200)
	var wr struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &wr))
	require.Equal(t, "rec-1", wr.ID)
	require.Equal(t, 1, wr.Version)

	mustStatus(t, e.doTOML(t, "POST", "/v1/syncable/rec-1", v2), 200)

	// The listing carries the id.
	require.Contains(t, e.syncableIDs(t), "rec-1")

	// Two versions, newest known to the version reads.
	var versions []struct {
		Version int `json:"version"`
	}
	e.getJSON(t, "/v1/syncable/rec-1/versions", &versions)
	require.Len(t, versions, 2)

	var got struct {
		Data string `json:"data"`
	}
	e.getJSON(t, "/v1/syncable/rec-1/versions/1", &got)
	require.Equal(t, v1, got.Data)

	// Rollback to v1 creates v3 with v1's content.
	mustStatus(t, e.doEmpty(t, "POST", "/v1/syncable/rec-1/rollback?to=1"), 200)
	e.getJSON(t, "/v1/syncable/rec-1/versions/3", &got)
	require.Equal(t, v1, got.Data)
}

// TestDeleteSyncable deletes a real syncable and confirms the keepData flag
// round-trips in the response; afterwards the listing no longer carries it.
func TestDeleteSyncable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		keepData bool
	}{
		{"default tears down", "", false},
		{"keepData=true", "?keepData=true", true},
		{"keepData=false", "?keepData=false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngine(t)
			e.addType(t, "photos", "photos")
			e.addRecorderSyncable(t, "rec-1", "photos")

			w := e.doEmpty(t, "DELETE", "/v1/syncable/rec-1"+tc.query)
			mustStatus(t, w, 200)
			var body struct {
				ID       string `json:"id"`
				KeepData bool   `json:"keepData"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, "rec-1", body.ID)
			require.Equal(t, tc.keepData, body.KeepData)

			require.NotContains(t, e.syncableIDs(t), "rec-1")
		})
	}
}

// A non-boolean keepData is a 400 and deletes nothing — the syncable
// remains listed.
func TestDeleteSyncable_InvalidKeepData(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doEmpty(t, "DELETE", "/v1/syncable/rec-1?keepData=maybe")
	requireEnvelope(t, w, 400, "invalid_parameter")

	require.Contains(t, e.syncableIDs(t), "rec-1", "a rejected delete must not delete")
}

// TestRebuildSyncable triggers a real rebuild: 202, the ack body, and the
// Location header pointing at the status resource.
func TestRebuildSyncable(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doEmpty(t, "POST", "/v1/syncable/rec-1/rebuild")
	mustStatus(t, w, 202)

	// The ack body: an empty 202 field-read as a routing failure (an operator
	// spent half an hour disbelieving a success next to wrong-verb 405s). The
	// body confirms the trigger and names the poll target.
	var body http.SyncableRebuildResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "rec-1", body.ID)
	require.Equal(t, "rebuilding", body.Status)
	require.Equal(t, "/v1/syncable/rec-1/status", body.Poll)
	require.Equal(t, "/v1/syncable/rec-1/status", w.Header().Get("Location"),
		"202 carries the idiomatic Location header pointing at the status resource")
}

// An unknown syncable rebuilds to 404 — a real engine's own refusal.
func TestRebuildSyncable_NotFound(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "POST", "/v1/syncable/nope/rebuild")
	mustStatus(t, w, 404)
}

// TestRematerializeSyncable_NotRematerializable: the recorder sink cannot
// converge a replay in place, so the real engine refuses with 409 — the
// real path for the not_rematerializable mapping.
func TestRematerializeSyncable_NotRematerializable(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doEmpty(t, "POST", "/v1/syncable/rec-1/rematerialize")
	requireEnvelope(t, w, 409, "not_rematerializable")
}

// TestSyncableDeadLetterJourney is the operator's whole incident, end to end
// through the real pipeline: a sink that rejects a row wedges the worker
// (stuck surfaces on status), the manual dead-letter lever skips it (202 with
// the blocked index), the record lists with its kind and RFC3339 timestamp,
// a replay against the still-broken sink fails 502 with the cause and leaves
// the record, acknowledge marks it resolved-but-listable, and a replay after
// the sink recovers clears it entirely.
func TestSyncableDeadLetterJourney(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	// Wedge: the sink rejects the row; the worker retries and flags itself
	// stuck past the (test-shortened) threshold.
	e.sink.setErr(fmt.Errorf("downstream rejected the row"))
	e.proposeRow(t, "photos", "k1")
	st := e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		stuck, _ := m["stuck"].(bool)
		return stuck
	}, "stuck=true after the sink wedged")
	require.Equal(t, "running", st["workerState"], "a transiently-stuck worker is still running")
	require.Contains(t, st["message"], "downstream rejected the row")

	// The manual lever targets the blocked index.
	w := e.doEmpty(t, "POST", "/v1/syncable/rec-1/deadletter")
	mustStatus(t, w, 202)
	var dl struct {
		Index uint64 `json:"index"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &dl))
	require.NotZero(t, dl.Index)

	// The record lands once the worker honors the skip; the listing renders
	// kind and an RFC3339 timestamp, and status counts it while un-wedging.
	var errsList []struct {
		Index        uint64 `json:"index"`
		Timestamp    string `json:"timestamp"`
		Kind         string `json:"kind"`
		Acknowledged bool   `json:"acknowledged"`
	}
	require.Eventually(t, func() bool {
		e.getJSON(t, "/v1/syncable/rec-1/errors", &errsList)
		return len(errsList) == 1
	}, 15*time.Second, 10*time.Millisecond, "the manual dead letter never listed")
	require.Equal(t, dl.Index, errsList[0].Index)
	require.Equal(t, "manual", errsList[0].Kind)
	require.NotEmpty(t, errsList[0].Timestamp)
	require.False(t, errsList[0].Acknowledged)
	e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		stuck, _ := m["stuck"].(bool)
		n, _ := m["deadLetters"].(float64)
		return !stuck && n == 1
	}, "un-wedged with one dead letter counted")

	// Replay against the still-broken sink: 502, cause surfaced, record kept.
	w = e.doEmpty(t, "POST", fmt.Sprintf("/v1/syncable/rec-1/replay/%d", dl.Index))
	requireEnvelope(t, w, 502, "replay_failed")
	require.Contains(t, w.Body.String(), "downstream rejected the row",
		"the failure cause should be surfaced in details")

	// Acknowledge: resolved out-of-band, still listable as an audit trail —
	// and the status counts split (acknowledged records leave the
	// completeness count).
	mustStatus(t, e.doEmpty(t, "POST", fmt.Sprintf("/v1/syncable/rec-1/deadletter/%d/acknowledge", dl.Index)), 200)
	e.getJSON(t, "/v1/syncable/rec-1/errors", &errsList)
	require.Len(t, errsList, 1)
	require.True(t, errsList[0].Acknowledged)
	e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		n, _ := m["deadLetters"].(float64)
		acked, _ := m["acknowledgedDeadLetters"].(float64)
		return n == 0 && acked == 1
	}, "acknowledged record leaves the completeness count")

	// Fix the sink; a successful replay clears the record entirely.
	e.sink.setErr(nil)
	mustStatus(t, e.doEmpty(t, "POST", fmt.Sprintf("/v1/syncable/rec-1/replay/%d", dl.Index)), 200)
	e.getJSON(t, "/v1/syncable/rec-1/errors", &errsList)
	require.Empty(t, errsList)
	require.Equal(t, 1, e.sink.count(), "the replay delivered the row")
}

// TestDeadLetterStuckSyncable_NotStuck: the lever 409s when nothing is
// blocked — a healthy syncable through the real engine.
func TestDeadLetterStuckSyncable_NotStuck(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doEmpty(t, "POST", "/v1/syncable/rec-1/deadletter")
	requireEnvelope(t, w, 409, "not_stuck")
}

// TestGetSyncableErrors_Defaults: an existing syncable with no dead letters
// serializes as [] (not null).
func TestGetSyncableErrors_Defaults(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doEmpty(t, "GET", "/v1/syncable/rec-1/errors")
	mustStatus(t, w, 200)
	require.Equal(t, "[]", w.Body.String())
}

// TestGetSyncableErrors_UnknownIs404: the existence gate — an unknown id's
// empty list must not read as a healthy syncable.
func TestGetSyncableErrors_UnknownIs404(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "GET", "/v1/syncable/nope/errors")
	requireEnvelope(t, w, 404, "not_found")
}

// TestGetSyncableErrors_BadParams: invalid cursor params are rejected with
// 400 (the syncable exists, so the 404 gate does not mask them).
func TestGetSyncableErrors_BadParams(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")
	for _, path := range []string{
		"/v1/syncable/rec-1/errors?since=notanumber",
		"/v1/syncable/rec-1/errors?limit=0",
		"/v1/syncable/rec-1/errors?limit=-3",
		"/v1/syncable/rec-1/errors?limit=abc",
	} {
		t.Run(path, func(t *testing.T) {
			requireEnvelope(t, e.doEmpty(t, "GET", path), 400, "invalid_parameter")
		})
	}
}

// TestReplaySyncableDeadLetter_NotDeadLettered: replaying an index that is
// not a dead letter 404s through the real engine.
func TestReplaySyncableDeadLetter_NotDeadLettered(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doEmpty(t, "POST", "/v1/syncable/rec-1/replay/7")
	requireEnvelope(t, w, 404, "not_dead_lettered")
}

// TestReplaySyncableDeadLetter_BadIndex rejects a non-numeric index with 400.
func TestReplaySyncableDeadLetter_BadIndex(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "POST", "/v1/syncable/rec-1/replay/notanumber")
	requireEnvelope(t, w, 400, "invalid_parameter")
}
