package http_test

import (
	"encoding/json"
	"errors"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

func doSyncableStatus(t *testing.T, fake *clusterfakes.FakeCluster) (http.SyncableStatusResponse, string) {
	t.Helper()
	fake.SyncableExistsReturns(true, nil)
	h := http.New(fake)
	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/s1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var body http.SyncableStatusResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	return body, string(bs)
}

// A caught-up syncable that has skipped rows must not read as fully green:
// the status carries the dead-letter count and the latest skipped index, so
// the honest completeness check is caughtUp && deadLetters == 0. Field
// validation surfaced exactly this blind spot — an operator's convergence
// check passed while rows had been auto-skipped, discoverable only by
// scraping logs.
func TestSyncableStatus_SurfacesDeadLetters(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.SyncableProgressReturns(100, 100, nil) // caught up
	fake.SyncableDeadLetterStatsReturns(53, 0, 4242, nil)

	body, _ := doSyncableStatus(t, fake)

	require.True(t, body.CaughtUp)
	require.Equal(t, uint64(53), body.DeadLetters,
		"a caught-up sink with skips must expose the count")
	require.Equal(t, uint64(4242), body.LastDeadLetterIndex)
}

// The clean case: deadLetters is always present (0), lastDeadLetterIndex is
// omitted entirely.
func TestSyncableStatus_NoDeadLetters(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.SyncableProgressReturns(100, 100, nil)
	// SyncableDeadLetterStats defaults to (0, 0, nil).

	body, raw := doSyncableStatus(t, fake)

	require.Zero(t, body.DeadLetters)
	require.Contains(t, raw, `"deadLetters":0`, "the zero count must still be present")
	require.False(t, strings.Contains(raw, "lastDeadLetterIndex"),
		"lastDeadLetterIndex must be omitted when nothing was skipped")
}

// A config that EXISTS but failed to BUILD on this node (degraded — e.g. a
// missing ${VAR}) must not report workerState "running": "running" is
// derived from the absence of a park record, which a never-started worker
// satisfies vacuously — the same absence-derived lie the 404 gate closed
// for absent ids, one door over. The status consults the answering node's
// degraded-config record and reports "degraded" plus the (already
// redacted) build error.
func TestSyncableStatus_DegradedBuildIsNotRunning(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.SyncableProgressReturns(0, 87, nil) // checkpoint frozen at 0, head real — the lying shape
	fake.ConfigBuildErrorsReturns([]cluster.ConfigBuildError{
		{Kind: "ingestable", ID: "s1", Error: "not this one — kind must match"},
		{Kind: "syncable", ID: "other", Error: "not this one — id must match"},
		{Kind: "syncable", ID: "s1", Error: "interpolate: variable SINK_DSN not set"},
	})

	body, _ := doSyncableStatus(t, fake)

	require.Equal(t, cluster.WorkerStateDegraded, body.WorkerState,
		"a build-degraded config must not read as a running worker")
	require.Equal(t, "interpolate: variable SINK_DSN not set", body.Message,
		"the redacted build error tells the operator what to fix")
	require.False(t, body.CaughtUp,
		"a degraded config with pending head must not read caught up")
}

// The degraded check is scoped to this id and kind: other configs'
// degraded records must not leak into a healthy syncable's status.
func TestSyncableStatus_OtherDegradedRecordsDoNotLeak(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.SyncableProgressReturns(100, 100, nil)
	fake.ConfigBuildErrorsReturns([]cluster.ConfigBuildError{
		{Kind: "syncable", ID: "other", Error: "someone else's problem"},
		{Kind: "ingestable", ID: "s1", Error: "same id, different kind"},
	})

	body, _ := doSyncableStatus(t, fake)

	require.Equal(t, cluster.WorkerStateRunning, body.WorkerState)
	require.Empty(t, body.Message)
}

// doSyncableStatusRaw hits GET /v1/syncable/s1/status with an arbitrary query
// string and headers, returning the status code and raw body — the readPosition
// tests need non-200 outcomes and byte-level field presence checks.
func doSyncableStatusRaw(t *testing.T, fake *clusterfakes.FakeCluster, query string, headers map[string]string) (int, string) {
	t.Helper()
	h := http.New(fake)
	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/s1/status"+query, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	resp := w.Result()
	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(bs)
}

// ownerNode is always present — it tells the operator whose worker (and logs)
// to look at — while readPosition stays opt-in: the default call must remain
// the documented local O(1) poll-safe read, so even a node that COULD answer
// the position locally must not volunteer it unasked.
func TestSyncableStatus_OwnerNodeAlwaysPresent_ReadPositionOptIn(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(1)
	fake.SyncableOwnerReturns(1)
	fake.SyncableReadPositionReturns(777, true)
	fake.SyncableProgressReturns(50, 100, nil)

	status, raw := doSyncableStatusRaw(t, fake, "", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"ownerNode":1`)
	require.NotContains(t, raw, "readPosition",
		"the default call must not carry the position — opt-in only")
}

// The owner answers ?readPosition=true locally: no proxy hop, the live
// position in the body.
func TestSyncableStatus_ReadPositionOwnerLocal(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(1)
	fake.SyncableOwnerReturns(1)
	fake.SyncableReadPositionReturns(777, true)
	fake.SyncableProgressReturns(50, 100, nil)

	status, raw := doSyncableStatusRaw(t, fake, "?readPosition=true", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"readPosition":777`)
	require.Zero(t, fake.MemberAPIURLCallCount(), "the owner must not proxy to itself")
}

// A present-but-zero position is a real datum — "the worker has examined
// nothing yet", the exact split the phantom-adoption incident needed — so the
// pointer field must serialize 0 rather than omit it.
func TestSyncableStatus_ReadPositionZeroIsPresent(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(1)
	fake.SyncableOwnerReturns(1)
	fake.SyncableReadPositionReturns(0, true)
	fake.SyncableProgressReturns(0, 100, nil)

	status, raw := doSyncableStatusRaw(t, fake, "?readPosition=true", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"readPosition":0`,
		"position 0 must be present-and-zero, not absent")
}

// A non-owner proxies ?readPosition=true to the owner's announced API URL and
// returns the owner's response verbatim, with the loop-guard marker and the
// query preserved — the load-balancer-pinned caller gets the owner's answer
// no matter which node it reached.
func TestSyncableStatus_ReadPositionProxiedToOwner(t *testing.T) {
	const ownerBody = `{"stuck":false,"workerState":"running","checkpointIndex":50,"headIndex":100,"lag":50,"caughtUp":false,"deadLetters":0,"ownerNode":1,"readPosition":777}`

	var (
		mu       sync.Mutex
		gotFwd   string
		gotQuery string
		gotPath  string
		gotCalls int
	)
	owner := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		mu.Lock()
		gotFwd = r.Header.Get("X-Committed-Forwarded")
		gotQuery = r.URL.RawQuery
		gotPath = r.URL.Path
		gotCalls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, ownerBody)
	}))
	defer owner.Close()

	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(2)
	fake.SyncableOwnerReturns(1)
	fake.MemberAPIURLReturns(owner.URL, true)

	status, raw := doSyncableStatusRaw(t, fake, "?readPosition=true", nil)
	require.Equal(t, 200, status)
	require.JSONEq(t, ownerBody, raw)
	require.Zero(t, fake.SyncableProgressCallCount(), "the non-owner must proxy, not answer locally")
	require.Equal(t, uint64(1), fake.MemberAPIURLArgsForCall(0), "the proxy targets the OWNER, not the leader")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, gotCalls)
	require.Equal(t, "/v1/syncable/s1/status", gotPath)
	require.Equal(t, "readPosition=true", gotQuery, "the opt-in must survive the hop")
	require.Equal(t, "1", gotFwd, "loop-guard marker must be set on the forwarded request")
}

// When the owner can't be reached, the response degrades SOFTLY: 200 with
// every replicated field served and readPosition simply absent — ownerNode
// says who to ask directly. Observability must not 503 the fields it does
// have (unlike leaderRead, whose leader-only data has no local substitute).
func TestSyncableStatus_ReadPositionDegradesWhenOwnerUnreachable(t *testing.T) {
	dead := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {}))
	deadURL := dead.URL
	dead.Close()

	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(2)
	fake.SyncableOwnerReturns(1)
	fake.MemberAPIURLReturns(deadURL, true)
	fake.SyncableProgressReturns(50, 100, nil)

	status, raw := doSyncableStatusRaw(t, fake, "?readPosition=true", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"checkpointIndex":50`, "replicated fields must still serve")
	require.Contains(t, raw, `"ownerNode":1`, "the degraded answer must say who has the position")
	require.NotContains(t, raw, "readPosition")
}

// A forwarded request landing on a non-owner (stale ownership view mid-
// election) must NOT forward again — it answers locally in the degraded
// shape. One hop, ever.
func TestSyncableStatus_ForwardedRequestNeverReforwarded(t *testing.T) {
	var calls int
	var mu sync.Mutex
	upstream := httptest.NewServer(httpgo.HandlerFunc(func(w httpgo.ResponseWriter, r *httpgo.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
	}))
	defer upstream.Close()

	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(2)
	fake.SyncableOwnerReturns(1)
	fake.MemberAPIURLReturns(upstream.URL, true)
	fake.SyncableProgressReturns(50, 100, nil)

	status, raw := doSyncableStatusRaw(t, fake, "?readPosition=true",
		map[string]string{"X-Committed-Forwarded": "1"})
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"ownerNode":1`)
	require.NotContains(t, raw, "readPosition")

	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, calls, "a forwarded request must never hop again")
}

func TestSyncableStatus_ReadPositionInvalidParam(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	status, raw := doSyncableStatusRaw(t, fake, "?readPosition=sideways", nil)
	require.Equal(t, 400, status)
	require.Contains(t, raw, "invalid_parameter")
}

// The phantom fix, pinned: an id that never existed (or was deleted) must
// 404 — not synthesize workerState running + real head/lag from
// default-zero reads (the field finding: a typo'd id in monitoring watched
// a healthy phantom forever). The gate fires BEFORE the readPosition proxy:
// a phantom must not be shipped to the owner either.
func TestSyncableStatus_UnknownID404s(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(false, nil)
	fake.SyncableProgressReturns(0, 12345678, nil) // the phantom ingredients

	status, raw := doSyncableStatusRaw(t, fake, "", nil)
	require.Equal(t, 404, status)
	require.Contains(t, raw, "not_found")
	require.NotContains(t, raw, "workerState", "no status fields may be synthesized for an absent id")
}

func TestSyncableStatus_UnknownID404sBeforeProxy(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(false, nil)
	fake.IDReturns(2)
	fake.SyncableOwnerReturns(1) // a proxy would target node 1

	status, _ := doSyncableStatusRaw(t, fake, "?readPosition=true", nil)
	require.Equal(t, 404, status)
	require.Zero(t, fake.MemberAPIURLCallCount(),
		"the 404 gate must fire before the readPosition proxy — never ship a phantom to the owner")
}

func TestSyncableErrors_UnknownID404s(t *testing.T) {
	h, fake := setupTest()
	fake.SyncableExistsReturns(false, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/syncable/never-existed/errors", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 404, w.Code)
	require.Zero(t, fake.SyncableDeadLettersCallCount(),
		"an unknown id's empty dead-letter list is indistinguishable from healthy — gate first")
}

// The acknowledge endpoint: 200 on success, 404 with not_dead_lettered for
// an index that was never dead-lettered, 400 for a malformed index — and the
// id/index thread through to the cluster.
func TestAcknowledgeSyncableDeadLetterEndpoint(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	do := func(path string) int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
		return w.Result().StatusCode
	}

	require.Equal(t, 200, do("http://localhost/v1/syncable/s1/deadletter/42/acknowledge"))
	require.Equal(t, 1, fake.AcknowledgeSyncableDeadLetterCallCount())
	_, gotID, gotIndex := fake.AcknowledgeSyncableDeadLetterArgsForCall(0)
	require.Equal(t, "s1", gotID)
	require.Equal(t, uint64(42), gotIndex)

	fake.AcknowledgeSyncableDeadLetterReturns(cluster.ErrNotDeadLettered)
	require.Equal(t, 404, do("http://localhost/v1/syncable/s1/deadletter/42/acknowledge"))

	require.Equal(t, 400, do("http://localhost/v1/syncable/s1/deadletter/not-a-number/acknowledge"))
}

// acknowledgedDeadLetters rides the status response (omitted at zero), so
// the operator sees resolved-out-of-band history without it polluting the
// completeness count.
func TestSyncableStatus_AcknowledgedDeadLettersSplit(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.SyncableProgressReturns(100, 100, nil)
	fake.SyncableDeadLetterStatsReturns(0, 3, 4242, nil)

	body, raw := doSyncableStatus(t, fake)

	require.Zero(t, body.DeadLetters, "acknowledged records leave the completeness count")
	require.Equal(t, uint64(3), body.AcknowledgedDeadLetters)
	require.Contains(t, raw, `"acknowledgedDeadLetters":3`)
}

// ?stages=true surfaces the owner-local per-stage output key counts — the
// silent-empty-stage triage read ("how many keys does billed-pairs
// hold?"). Absent without the opt-in, absent for stage-free syncables,
// same soft-degrade contract as readPosition.
func TestSyncableStatus_StageKeyCounts(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(1)
	fake.SyncableOwnerReturns(1)
	fake.SyncableProgressReturns(50, 100, nil)
	fake.SyncableStageStatsReturns(map[string]cluster.StageStat{
		"billed-pairs": {Keys: 0, Inputs: 496000, Fanned: 91737},
		"completed":    {Keys: 22023, Inputs: 700000},
	}, true)

	// Opted in: the stats render — keys plus the flow counters that split
	// region-not-reached / fan-empty / filtered-to-zero.
	status, raw := doSyncableStatusRaw(t, fake, "?stages=true", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"billed-pairs":{"keys":0,"inputs":496000,"fanned":91737}`,
		"an EMPTY stage is visible as keys 0 WITH its flow context")
	require.Contains(t, raw, `"completed":{"keys":22023,"inputs":700000}`)

	// Default call: no counts (poll-safe O(1) contract untouched).
	_, raw = doSyncableStatusRaw(t, fake, "", nil)
	require.NotContains(t, raw, "billed-pairs")

	// Stage-free syncable (or non-owner): field absent, not an error.
	fake.SyncableStageStatsReturns(nil, false)
	status, raw = doSyncableStatusRaw(t, fake, "?stages=true", nil)
	require.Equal(t, 200, status)
	require.NotContains(t, raw, `"stages"`)
}

// ?probeStage/?probeKey answers a single-key existence probe — the other
// half of stage introspection ("is THIS pair in billed-pairs?"). A
// typo'd stage name is a loud 400, never "key absent".
func TestSyncableStatus_StageKeyProbe(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(1)
	fake.SyncableOwnerReturns(1)
	fake.SyncableProgressReturns(50, 100, nil)

	fake.SyncableStageKeyExistsReturns(true, true, nil)
	status, raw := doSyncableStatusRaw(t, fake, "?probeStage=billed-pairs&probeKey=J-1&probeKey=5", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"stageKeyExists":true`)
	_, stage, parts := fake.SyncableStageKeyExistsArgsForCall(0)
	require.Equal(t, "billed-pairs", stage)
	require.Equal(t, []string{"J-1", "5"}, parts,
		"parts pass through VERBATIM — the key-space owner renders and composes; this layer knows no encodings")

	fake.SyncableStageKeyExistsReturns(false, true, nil)
	_, raw = doSyncableStatusRaw(t, fake, "?probeStage=billed-pairs&probeKey=nope", nil)
	require.Contains(t, raw, `"stageKeyExists":false`)

	// Unknown stage: loud 400.
	fake.SyncableStageKeyExistsReturns(false, true, errors.New(`stage "billed-pear" is not declared by this projection`))
	status, raw = doSyncableStatusRaw(t, fake, "?probeStage=billed-pear&probeKey=x", nil)
	require.Equal(t, 400, status)
	require.Contains(t, raw, "billed-pear")

	// Half a probe is a parameter error.
	status, _ = doSyncableStatusRaw(t, fake, "?probeStage=only", nil)
	require.Equal(t, 400, status)
}

// A worker mid-re-derivation must not read as plain "running" with
// silently climbing lag (the field finding: ~30 minutes of invisible
// re-derive after a store reset). The answering node reports
// workerState "re-deriving" with progress, on the DEFAULT call.
func TestSyncableStatus_Rederiving(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.SyncableExistsReturns(true, nil)
	fake.IDReturns(1)
	fake.SyncableOwnerReturns(1)
	fake.SyncableProgressReturns(36700000, 36760000, nil)
	fake.SyncableStageRecoveryReturns(12400000, 36700000, true)

	status, raw := doSyncableStatusRaw(t, fake, "", nil)
	require.Equal(t, 200, status)
	require.Contains(t, raw, `"workerState":"re-deriving"`)
	require.Contains(t, raw, `"stageRecovery":{"folded":12400000,"target":36700000}`)

	// No active re-derivation: plain running, field absent.
	fake.SyncableStageRecoveryReturns(0, 0, false)
	_, raw = doSyncableStatusRaw(t, fake, "", nil)
	require.Contains(t, raw, `"workerState":"running"`)
	require.NotContains(t, raw, "stageRecovery")
}
