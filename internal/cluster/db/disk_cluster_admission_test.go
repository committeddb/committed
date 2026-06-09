package db_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	clusterhttp "github.com/committeddb/committed/internal/cluster/http"
)

// createDiskTestDB is createDB plus extra options, with the disk coordinator
// disabled so its background tick can never recompute the verdict mid-test
// and race an injected one.
func createDiskTestDB(opts ...db.Option) *DB {
	id := uint64(1)
	peers := db.Peers{id: ""}
	s := NewMemoryStorage()
	all := append([]db.Option{
		db.WithTickInterval(testTickInterval),
		db.WithDiskReportInterval(-1),
	}, opts...)
	return &DB{db.New(id, peers, s, parser.New(), nil, nil, all...), s, peers, id}
}

// TestClusterDiskGate_VerdictDominatesLocalState asserts that a fresh
// cluster verdict overrides the node-local disk state in BOTH directions at
// the propose gate: a locally-healthy node rejects when the cluster is
// unsafe (this is what closes the follower→leader forwarding bypass — the
// follower never submits the proposal raft would forward), and a locally-full
// node admits when the leader says the cluster is healthy (the
// one-full-follower case: the write commits on the leader's quorum
// regardless of this node's disk).
func TestClusterDiskGate_VerdictDominatesLocalState(t *testing.T) {
	d := createDiskTestDB()
	defer d.Close()
	user := func() *cluster.Proposal { return createProposals([][]string{{"v"}})[0] }

	// Baseline commit: leadership is established and writes flow.
	require.NoError(t, d.Propose(testCtx(t), user()))

	// Fresh cluster-critical verdict on a locally-healthy node: user writes
	// are rejected with the same typed error HTTP maps to 507.
	d.SetClusterVerdictForTest("critical", "quorum at risk: 1 of 3 voters have disk headroom (need 2)", 9, 0)
	require.ErrorIs(t, d.Propose(testCtx(t), user()), cluster.ErrInsufficientStorage)

	// The Phase 1 kind layering applies cluster-wide: checkpoints still
	// flow under cluster-critical (and config would too).
	require.NoError(t, d.ProposeSyncableIndexForTest(testCtx(t), "s-1", 7))

	// Fresh ok verdict on a locally-FULL node: admitted anyway.
	d.SetLocalDiskStateForTest("full")
	d.SetClusterVerdictForTest("ok", "", 9, 0)
	require.NoError(t, d.Propose(testCtx(t), user()))

	// Cluster-full also freezes config cluster-wide, but never checkpoints.
	d.SetClusterVerdictForTest("full", "leader disk full", 9, 0)
	cfg := &cluster.Configuration{
		ID:       "person",
		MimeType: "text/toml",
		Data:     []byte("[type]\nname = \"Person\"\n[migration]\nnone = true"),
	}
	require.ErrorIs(t, d.ProposeType(testCtx(t), cfg), cluster.ErrInsufficientStorage)
	require.NoError(t, d.ProposeSyncableIndexForTest(testCtx(t), "s-1", 8))
}

// TestClusterDiskGate_StaleVerdictFallsBackToLocal asserts the bounded
// staleness contract: a verdict older than its TTL stops being enforced and
// the gate degrades to the node-local Phase 1 decision — in both directions.
func TestClusterDiskGate_StaleVerdictFallsBackToLocal(t *testing.T) {
	d := createDiskTestDB()
	defer d.Close()
	user := func() *cluster.Proposal { return createProposals([][]string{{"v"}})[0] }

	require.NoError(t, d.Propose(testCtx(t), user()))

	// Stale ok verdict + locally-full disk: the local gate rejects.
	d.SetLocalDiskStateForTest("full")
	d.SetClusterVerdictForTest("ok", "", 9, time.Hour)
	require.ErrorIs(t, d.Propose(testCtx(t), user()), cluster.ErrInsufficientStorage)

	// Stale critical verdict + locally-healthy disk: writes flow again.
	d.SetLocalDiskStateForTest("ok")
	d.SetClusterVerdictForTest("critical", "quorum at risk", 9, time.Hour)
	require.NoError(t, d.Propose(testCtx(t), user()))
}

// TestDiskAdmission_ReportsSourceAndDecision pins the GET /node/status
// surface: a fresh verdict reports source=cluster with the leader and
// reason; a stale one falls back to source=local reflecting the node's own
// disk; DiskState always reports the local level.
func TestDiskAdmission_ReportsSourceAndDecision(t *testing.T) {
	d := createDiskTestDB()
	defer d.Close()

	d.SetLocalDiskStateForTest("warn")
	d.SetClusterVerdictForTest("critical", "quorum at risk: 1 of 3 voters have disk headroom (need 2)", 9, 0)

	adm := d.DiskAdmission()
	require.False(t, adm.Admitted)
	require.Equal(t, "critical", adm.State)
	require.Equal(t, "cluster", adm.Source)
	require.Equal(t, uint64(9), adm.LeaderID)
	require.NotEmpty(t, adm.Reason)
	require.Equal(t, "warn", d.DiskState())

	d.SetClusterVerdictForTest("critical", "quorum at risk", 9, time.Hour)
	adm = d.DiskAdmission()
	require.True(t, adm.Admitted)
	require.Equal(t, "warn", adm.State)
	require.Equal(t, "local", adm.Source)
	require.Zero(t, adm.LeaderID)
}

// TestReportDisk_LeaderRecordsAndAnswers drives the leader half of the
// report round trip on a real single-node DB: reports are accepted once the
// node is leader, the returned verdict tracks the leader's own disk state,
// and junk states are rejected.
func TestReportDisk_LeaderRecordsAndAnswers(t *testing.T) {
	d := createDiskTestDB()
	defer d.Close()

	// Reports are rejected with ErrNotLeader until election completes, so
	// "first accepted report" doubles as the leadership barrier.
	require.Eventually(t, func() bool {
		_, err := d.ReportDisk(2, "full")
		return err == nil
	}, 5*time.Second, 5*time.Millisecond, "leader never started accepting disk reports")

	// Node 2 is not a voter in this single-node cluster, so its full report
	// can't move the verdict: the leader's own ok state decides.
	v, err := d.ReportDisk(2, "full")
	require.NoError(t, err)
	require.Equal(t, "ok", v.State)
	require.Equal(t, uint64(1), v.LeaderID)
	require.Empty(t, v.Reason)

	// The leader's own pressure shows up in the verdict it hands back.
	d.SetDiskStateForTest("critical")
	v, err = d.ReportDisk(2, "ok")
	require.NoError(t, err)
	require.Equal(t, "critical", v.State)
	require.Contains(t, v.Reason, "leader disk critical")

	// Unknown state: rejected outright (the HTTP layer maps this to 400).
	_, err = d.ReportDisk(2, "toasty")
	require.Error(t, err)
	require.NotErrorIs(t, err, cluster.ErrNotLeader)
}

// TestDiskReportSender_RoundTrip posts a report through the production HTTP
// sender against a real chi handler backed by a real DB — pinning the wire
// contract between db's diskReportWire/diskVerdictWire and the http layer's
// DiskReportRequest/DiskReportResponse, plus the bearer-token handshake.
func TestDiskReportSender_RoundTrip(t *testing.T) {
	d := createDiskTestDB()
	defer d.Close()
	h := clusterhttp.New(d, clusterhttp.WithBearerToken("cluster-secret"))
	srv := httptest.NewServer(h)
	defer srv.Close()

	send := db.HTTPDiskReportSenderForTest("cluster-secret")

	var verdict cluster.DiskVerdict
	require.Eventually(t, func() bool {
		v, err := send(testCtx(t), srv.URL, 2, "warn")
		if err != nil {
			return false
		}
		verdict = v
		return true
	}, 5*time.Second, 5*time.Millisecond, "report round trip never succeeded")
	require.Equal(t, "ok", verdict.State)
	require.Equal(t, uint64(1), verdict.LeaderID)

	// The leader's pressure travels back through the same round trip.
	d.SetDiskStateForTest("full")
	verdict, err := send(testCtx(t), srv.URL, 2, "warn")
	require.NoError(t, err)
	require.Equal(t, "full", verdict.State)
	require.Contains(t, verdict.Reason, "leader disk full")

	// A wrong token is rejected by the bearer middleware (401 → error).
	badSend := db.HTTPDiskReportSenderForTest("wrong-secret")
	_, err = badSend(testCtx(t), srv.URL, 2, "warn")
	require.ErrorContains(t, err, "401")
}

// TestDiskCoordinate_LeaderRecomputesAndTransfers runs whole coordinator
// cycles on a real leader: the verdict is recomputed from the leader's own
// state, the transfer trigger fires exactly when constrained with a healthy
// target, and the cooldown rate-limits the next attempt.
func TestDiskCoordinate_LeaderRecomputesAndTransfers(t *testing.T) {
	d := createDiskTestDB()
	defer d.Close()

	// Leadership barrier (coordinator's leader path needs IsLeader()).
	require.NoError(t, d.Propose(testCtx(t), createProposals([][]string{{"v"}})[0]))

	var transfers []uint64
	target := uint64(0)
	d.SetDiskTransferHooksForTest(
		func() uint64 { return target },
		func(id uint64) { transfers = append(transfers, id) },
		time.Minute,
	)

	// Healthy leader: cycle publishes an ok verdict, no transfer.
	d.DiskCoordinateForTest()
	adm := d.DiskAdmission()
	require.True(t, adm.Admitted)
	require.Equal(t, "cluster", adm.Source)
	require.Empty(t, transfers)

	// Constrained leader, no healthy target: rejects but keeps leadership.
	d.SetLocalDiskStateForTest("critical")
	d.DiskCoordinateForTest()
	adm = d.DiskAdmission()
	require.False(t, adm.Admitted)
	require.Equal(t, "critical", adm.State)
	require.Contains(t, adm.Reason, "leader disk critical")
	require.Empty(t, transfers)

	// A healthy target appears: the transfer fires once...
	target = 2
	d.DiskCoordinateForTest()
	require.Equal(t, []uint64{2}, transfers)

	// ...and the cooldown swallows the immediate retry.
	d.DiskCoordinateForTest()
	require.Len(t, transfers, 1)

	// Recovery: verdict returns to ok and admission resumes.
	d.SetLocalDiskStateForTest("ok")
	d.DiskCoordinateForTest()
	adm = d.DiskAdmission()
	require.True(t, adm.Admitted)
	require.Equal(t, "ok", adm.State)
}
