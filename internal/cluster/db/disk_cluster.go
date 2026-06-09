package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// Cluster-aware disk admission (Phase 2 of the disk guardrail).
//
// The Phase 1 gate is node-local: it rejects on whichever node receives a
// proposal, based on that node's own disk. But every node stores the full
// replicated log — a node fills from the leader's replication stream no
// matter where writes enter — so admission has to be a cluster decision:
//
//	admit a write iff the leader is healthy AND at least a quorum of
//	voters are healthy (healthy = above the critical threshold).
//
// The leader computes that verdict from its own disk plus the disk states
// members report. Reports travel out of band — over the existing HTTP API,
// never through a raft proposal (a node can't reliably write "I'm full" when
// it is full): each member periodically POSTs its state to the leader's
// announced API URL, and the response carries the current verdict back, so
// one round trip per member per interval keeps both directions fresh. Every
// node then enforces the verdict at its own propose gate (checkDiskWritable),
// which also closes the Phase 1 leak where a follower forwarded proposals to
// a full leader over the raft transport, bypassing the leader's gate.
//
// When no fresh verdict is available (no leader, leader has no announced API
// URL, reports failing), the gate falls back to the Phase 1 node-local
// decision — the cluster degrades to the old behavior, never below it.

// DefaultDiskReportInterval is the production cadence at which a member
// reports its disk state to the leader (and at which the leader recomputes
// the verdict). The verdict a node enforces is therefore at most
// diskVerdictTTLFactor× this stale; 10s keeps that window minutes ahead of
// any realistic disk-fill while costing one tiny HTTP round trip per node.
const DefaultDiskReportInterval = 10 * time.Second

// diskVerdictTTLFactor scales the report interval into the freshness window
// for both a follower's cached verdict and the leader's view of a member
// report. 3× tolerates two missed rounds before falling back (follower) or
// assuming ok (leader).
const diskVerdictTTLFactor = 3

// defaultDiskTransferCooldown rate-limits disk-pressure leadership transfers
// so a leader flapping around the critical threshold doesn't bounce
// leadership across the cluster every report interval.
const defaultDiskTransferCooldown = time.Minute

// diskReport is one member's last reported disk state, leader-side. at is
// the receipt time; a report older than the verdict TTL is treated as ok
// (fail-open: a member that stops reporting — crashed, partitioned, or
// running an older version — degrades the math to Phase 1 behavior rather
// than freezing a healthy cluster).
type diskReport struct {
	state diskState
	at    time.Time
}

// diskVerdictState is the cached admission verdict a node's propose gate
// enforces: on the leader it is recomputed locally every coordinator tick,
// on every report received, and on every local disk-state change; on a
// follower it is whatever the leader returned on the last report round trip.
// reasonCode is the bounded enum for the admission metric ("ok",
// "leader_disk", "quorum_at_risk"); reason is the operator-facing message.
type diskVerdictState struct {
	state      diskState
	reason     string
	reasonCode string
	leaderID   uint64
	at         time.Time
}

// diskReportSender delivers one disk report to the leader's API and returns
// the verdict the leader responded with. Production uses httpDiskReporter;
// tests inject a fake to drive multi-node admission scenarios in-process.
type diskReportSender func(ctx context.Context, leaderURL string, nodeID uint64, state string) (cluster.DiskVerdict, error)

// parseDiskState maps the wire form back to a diskState. Reports travel
// between nodes as strings (the JSON API surface), so the leader re-parses
// on receipt; an unknown level is rejected rather than guessed.
func parseDiskState(s string) (diskState, bool) {
	switch s {
	case "ok":
		return diskOK, true
	case "warn":
		return diskWarn, true
	case "critical":
		return diskCritical, true
	case "full":
		return diskFull, true
	}
	return diskOK, false
}

// diskVerdictTTL is the freshness window for the cached verdict and for
// member reports. Derived from the report interval so shortening the
// interval (tests, aggressive ops) tightens staleness proportionally.
func (db *DB) diskVerdictTTL() time.Duration {
	interval := db.diskReportInterval
	if interval <= 0 {
		interval = DefaultDiskReportInterval
	}
	return diskVerdictTTLFactor * interval
}

// admissionDiskState returns the disk-pressure level the propose gate should
// enforce right now and, when that decision comes from a fresh cluster
// verdict, the verdict itself (nil means node-local fallback). The cluster
// verdict — when fresh — dominates the local state in BOTH directions: a
// full follower admits writes a healthy cluster can serve (they commit on
// the leader's quorum regardless of this node's disk), and a healthy node
// rejects writes the cluster has deemed unsafe (closing the follower→leader
// forwarding bypass).
func (db *DB) admissionDiskState(now time.Time) (diskState, *diskVerdictState) {
	if v := db.diskVerdict.Load(); v != nil && now.Sub(v.at) <= db.diskVerdictTTL() {
		return v.state, v
	}
	return diskState(db.diskState.Load()), nil
}

// checkDiskWritable returns the typed error a proposal of the given kind
// should be rejected with under the current admission decision, or nil if it
// may proceed. The hot path is an atomic pointer load + comparison; with no
// watcher and no verdict it always returns nil.
func (db *DB) checkDiskWritable(kind string) error {
	state, _ := db.admissionDiskState(time.Now())
	return diskRejection(state, kind)
}

// DiskState returns this node's own disk-pressure level as last sampled by
// the local disk watcher. Implements cluster.Cluster for GET /node/status.
func (db *DB) DiskState() string {
	return diskState(db.diskState.Load()).String()
}

// DiskAdmission returns the write-admission decision this node's propose
// gate is applying right now — the fresh cluster verdict when one is held,
// or the node-local fallback. Implements cluster.Cluster for GET /node/status.
func (db *DB) DiskAdmission() cluster.DiskAdmissionStatus {
	state, v := db.admissionDiskState(time.Now())
	st := cluster.DiskAdmissionStatus{
		Admitted: state < diskCritical,
		State:    state.String(),
		Source:   "local",
	}
	if v != nil {
		st.Reason = v.reason
		st.Source = "cluster"
		st.LeaderID = v.leaderID
	} else if state >= diskCritical {
		st.Reason = "node-local disk " + state.String()
	}
	return st
}

// ReportDisk records member nodeID's disk state and returns the freshly
// recomputed cluster verdict. Implements cluster.Cluster; powers
// POST /v1/node/disk-report. Only the leader aggregates — on any other node
// it returns cluster.ErrNotLeader so the reporter re-resolves the leader on
// its next cycle.
func (db *DB) ReportDisk(nodeID uint64, state string) (cluster.DiskVerdict, error) {
	s, ok := parseDiskState(state)
	if !ok {
		return cluster.DiskVerdict{}, fmt.Errorf("unknown disk state %q", state)
	}
	if db.leaderState == nil || !db.leaderState.IsLeader() {
		return cluster.DiskVerdict{LeaderID: db.Leader()}, cluster.ErrNotLeader
	}

	now := time.Now()
	db.diskReportsMu.Lock()
	db.diskReports[nodeID] = diskReport{state: s, at: now}
	db.diskReportsMu.Unlock()

	v := db.recomputeDiskVerdict(now)
	return cluster.DiskVerdict{State: v.state.String(), Reason: v.reason, LeaderID: v.leaderID}, nil
}

// recomputeDiskVerdict computes, publishes, and returns the cluster verdict
// from this node's (the leader's) own disk state plus the collected member
// reports. Called on the coordinator tick, on every report received, and on
// a local disk-state change — so the leader's cached verdict is never older
// than one report interval.
func (db *DB) recomputeDiskVerdict(now time.Time) *diskVerdictState {
	v := db.computeDiskVerdict(now)
	db.diskVerdict.Store(v)
	db.publishAdmissionMetrics()
	return v
}

// computeDiskVerdict gathers this node's inputs — its own state, the voter
// set, the collected reports — and runs them through the quorum math.
func (db *DB) computeDiskVerdict(now time.Time) *diskVerdictState {
	local := diskState(db.diskState.Load())
	voters, _, _ := db.raft.memberStatus()

	db.diskReportsMu.Lock()
	defer db.diskReportsMu.Unlock()
	return diskVerdictFrom(db.ID(), local, voters, db.diskReports, now, db.diskVerdictTTL())
}

// diskVerdictFrom is the quorum math. The cluster-effective state is
//
//	max(leader's own state, the q-th healthiest voter state)
//
// where q is the quorum size: the q-th healthiest voter state is the best
// pressure level a full quorum can collectively stay under, so it exceeds
// critical exactly when no healthy quorum exists. Taking the max with the
// leader's own state folds in "the leader must be healthy" (a full leader
// rejects even when a healthy quorum of followers exists — every write
// lands on the leader's disk first). The same kind layering as Phase 1 then
// applies cluster-wide via diskRejection: critical rejects user data, full
// also freezes config, checkpoints always flow.
//
// A voter with no fresh report counts as ok (fail-open): a crashed or
// partitioned member must not freeze a cluster that still has a healthy
// quorum, and a cluster whose reports are broken degrades to Phase 1's
// node-local behavior rather than below it. Stale and departed-member
// entries are pruned from reports in place — the caller holds diskReportsMu.
func diskVerdictFrom(selfID uint64, local diskState, voters map[uint64]struct{},
	reports map[uint64]diskReport, now time.Time, ttl time.Duration,
) *diskVerdictState {
	v := &diskVerdictState{state: local, reasonCode: "ok", leaderID: selfID, at: now}

	for id, r := range reports {
		if _, isVoter := voters[id]; !isVoter || now.Sub(r.at) > ttl {
			delete(reports, id)
		}
	}

	if len(voters) == 0 {
		// Not yet part of a settled configuration — node-local is all we have.
		if local >= diskCritical {
			v.reason = "leader disk " + local.String()
			v.reasonCode = "leader_disk"
		}
		return v
	}

	states := make([]diskState, 0, len(voters))
	for id := range voters {
		if id == selfID {
			states = append(states, local)
			continue
		}
		r, ok := reports[id]
		if !ok {
			states = append(states, diskOK)
			continue
		}
		states = append(states, r.state)
	}

	slices.Sort(states)
	quorum := len(states)/2 + 1
	quorumState := states[quorum-1]

	healthy := 0
	for _, s := range states {
		if s < diskCritical {
			healthy++
		}
	}

	v.state = max(local, quorumState)
	switch {
	case local >= diskCritical:
		v.reason = "leader disk " + local.String()
		v.reasonCode = "leader_disk"
	case quorumState >= diskCritical:
		v.reason = fmt.Sprintf("quorum at risk: %d of %d voters have disk headroom (need %d)",
			healthy, len(states), quorum)
		v.reasonCode = "quorum_at_risk"
	}
	return v
}

// publishAdmissionMetrics records this node's current effective admission
// view: the cluster-level disk state gauge plus the writes-admitted gauge
// with its bounded reason code ("ok", "leader_disk", "quorum_at_risk", or
// "local_fallback" when no fresh verdict is held).
func (db *DB) publishAdmissionMetrics() {
	if db.metrics == nil {
		return
	}
	state, v := db.admissionDiskState(time.Now())
	code := "local_fallback"
	if v != nil {
		code = v.reasonCode
	}
	db.metrics.SetDiskClusterState(state.String())
	db.metrics.SetWriteAdmission(state < diskCritical, code)
}

// nudgeDiskCoordinator wakes the coordinator ahead of its next tick — called
// on a local disk-state change so a follower reports the transition (and
// picks up the resulting verdict) immediately instead of up to one interval
// late. Non-blocking: a pending nudge is enough.
func (db *DB) nudgeDiskCoordinator() {
	select {
	case db.diskNudgeC <- struct{}{}:
	default:
	}
}

// diskCoordinator is the per-node admission loop, started from db.New and
// stopped by db.Close via db.ctx. Each cycle: the leader recomputes the
// verdict and considers transferring leadership away from its own disk
// pressure; a follower reports its disk state to the leader's announced API
// URL and caches the verdict the response carries. Disabled (never started)
// when the report interval is configured to 0.
func (db *DB) diskCoordinator() {
	ticker := time.NewTicker(db.diskReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-db.diskNudgeC:
		case <-db.ctx.Done():
			return
		}
		db.diskCoordinate(time.Now())
	}
}

// diskCoordinate runs one coordinator cycle. Split from the loop so a local
// disk-state change (onDiskState) can run a synchronous cycle on the leader —
// keeping the verdict the gate reads consistent with the state the watcher
// just published, with no window where a test (or a burst of proposals)
// observes the old verdict.
func (db *DB) diskCoordinate(now time.Time) {
	if db.leaderState != nil && db.leaderState.IsLeader() {
		v := db.recomputeDiskVerdict(now)
		db.maybeTransferLeadership(now, v)
		return
	}
	db.reportDiskToLeader(now)
	db.publishAdmissionMetrics()
}

// reportDiskToLeader performs the follower half of one cycle: resolve the
// leader's announced API URL, POST this node's disk state, and cache the
// verdict from the response. Any failure leaves the cached verdict to age
// out, after which the gate falls back to the node-local decision — so the
// failure modes (no leader, no announced URL, leader unreachable, leadership
// just moved) all degrade to Phase 1 behavior. Failures log at Debug: a
// persistent one shows up as the local_fallback admission metric, not log
// spam every interval.
func (db *DB) reportDiskToLeader(now time.Time) {
	leaderID := db.Leader()
	if leaderID == 0 || leaderID == db.ID() {
		return
	}
	leaderURL, ok := db.MemberAPIURL(leaderID)
	if !ok || leaderURL == "" {
		return
	}

	state := diskState(db.diskState.Load())
	ctx, cancel := context.WithTimeout(db.ctx, defaultDiskReportTimeout)
	defer cancel()
	verdict, err := db.diskReportSend(ctx, leaderURL, db.ID(), state.String())
	if err != nil {
		db.logger.Debug("disk report to leader failed",
			zap.Uint64("leader", leaderID), zap.Error(err))
		return
	}

	db.applyReportedVerdict(verdict, now)
}

// applyReportedVerdict caches a verdict the leader returned on a report
// round trip, making it the decision this node's propose gate enforces for
// the next TTL window. Logs only on a state change so the steady-state
// report loop is silent.
func (db *DB) applyReportedVerdict(verdict cluster.DiskVerdict, now time.Time) {
	s, ok := parseDiskState(verdict.State)
	if !ok {
		db.logger.Debug("disk report: leader returned unknown verdict state",
			zap.String("state", verdict.State))
		return
	}
	code := "ok"
	if s >= diskCritical {
		code = "cluster_reject"
	}
	prev := db.diskVerdict.Load()
	db.diskVerdict.Store(&diskVerdictState{
		state:      s,
		reason:     verdict.Reason,
		reasonCode: code,
		leaderID:   verdict.LeaderID,
		at:         now,
	})
	if prev == nil || prev.state != s {
		db.logger.Info("cluster write-admission verdict changed",
			zap.String("state", s.String()),
			zap.String("reason", verdict.Reason),
			zap.Uint64("leader", verdict.LeaderID))
	}
}

// maybeTransferLeadership moves the leader role off this node when its own
// disk is the constraint and a confirmed-healthy voter exists — converting
// "leader full" (cluster rejects all writes) into "follower full" (cluster
// keeps admitting via the healthy quorum). Guarded against flapping: only a
// target with a FRESH report below critical qualifies (never an assumed-ok
// silent member), and transfers are rate-limited by the cooldown.
func (db *DB) maybeTransferLeadership(now time.Time, v *diskVerdictState) {
	local := diskState(db.diskState.Load())
	if local < diskCritical {
		return
	}
	if now.Sub(db.lastDiskTransfer) < db.diskTransferCooldown {
		return
	}
	target := db.pickTransferTargetFn(now)
	if target == 0 {
		return
	}

	verdict := local
	if v != nil {
		verdict = v.state
	}
	db.logger.Warn("leader disk constrained; transferring leadership to a healthy voter",
		zap.String("disk_state", local.String()),
		zap.Uint64("target", target),
		zap.String("verdict", verdict.String()))
	if db.metrics != nil {
		db.metrics.DiskLeadershipTransfer()
	}
	db.lastDiskTransfer = now
	db.transferLeadershipFn(target)
}

// pickTransferTarget gathers the leader-side inputs (voter set, replication
// progress, collected reports) for pickDiskTransferTarget. It is the default
// value of db.pickTransferTargetFn.
func (db *DB) pickTransferTarget(now time.Time) uint64 {
	voters, _, _ := db.raft.memberStatus()
	_, _, _, _, views := db.raft.membershipView()
	match := make(map[uint64]uint64, len(views))
	for _, mv := range views {
		match[mv.id] = mv.match
	}

	db.diskReportsMu.Lock()
	defer db.diskReportsMu.Unlock()
	return pickDiskTransferTarget(db.ID(), voters, db.diskReports, match, now, db.diskVerdictTTL())
}

// pickDiskTransferTarget chooses the voter to hand leadership to: among
// voters with a fresh report strictly below critical, the one with the most
// disk headroom (lowest state), ties broken by replication progress (highest
// match index, so the transfer's catch-up phase is shortest). Returns 0 when
// no confirmed-healthy voter exists — better to keep a constrained leader
// (checkpoints and compaction still run) than to hand the cluster to a node
// in the same trouble, or to one we know nothing about.
func pickDiskTransferTarget(selfID uint64, voters map[uint64]struct{},
	reports map[uint64]diskReport, match map[uint64]uint64, now time.Time, ttl time.Duration,
) uint64 {
	var target uint64
	var targetState diskState
	for id := range voters {
		if id == selfID {
			continue
		}
		r, ok := reports[id]
		if !ok || now.Sub(r.at) > ttl || r.state >= diskCritical {
			continue
		}
		if target == 0 || r.state < targetState ||
			(r.state == targetState && match[id] > match[target]) {
			target = id
			targetState = r.state
		}
	}
	return target
}

// defaultDiskReportTimeout bounds one report round trip. Sized like the
// leader-read proxy hop: well under the report interval, so a wedged leader
// costs one missed round, not a backed-up coordinator.
const defaultDiskReportTimeout = 5 * time.Second

// httpDiskReporter is the production diskReportSender: it POSTs the report
// to the leader's announced API URL with the cluster's bearer token and
// decodes the verdict from the response. The client (TLS trust for
// self-signed peer APIs) and token are wired from cmd/node.go via
// WithDiskReportHTTP; the zero value works for plaintext, unauthenticated
// dev clusters.
type httpDiskReporter struct {
	client *nethttp.Client
	token  string
}

// newHTTPDiskReportSender builds the production sender from the wired client
// and token (both optional). A nil client gets a timeout-bounded default with
// system-root TLS — correct for plaintext or publicly-signed peer APIs, same
// default as the leader-read proxy.
func newHTTPDiskReportSender(client *nethttp.Client, token string) diskReportSender {
	if client == nil {
		client = &nethttp.Client{Timeout: defaultDiskReportTimeout}
	}
	reporter := &httpDiskReporter{client: client, token: token}
	return reporter.send
}

// diskReportWire is the JSON body of POST /v1/node/disk-report, and
// diskVerdictWire its response — mirrored by the HTTP layer's handler types.
// Kept in sync by TestDiskReportSender_RoundTrip, which posts through a real
// handler. (db can't import internal/cluster/http: layering, not a cycle.)
type diskReportWire struct {
	Node  uint64 `json:"node"`
	State string `json:"state"`
}

type diskVerdictWire struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
	Leader uint64 `json:"leader"`
}

func (r *httpDiskReporter) send(ctx context.Context, leaderURL string, nodeID uint64, state string) (cluster.DiskVerdict, error) {
	target, err := url.JoinPath(leaderURL, "/v1/node/disk-report")
	if err != nil {
		return cluster.DiskVerdict{}, fmt.Errorf("disk report: join leader url: %w", err)
	}
	body, err := json.Marshal(diskReportWire{Node: nodeID, State: state})
	if err != nil {
		return cluster.DiskVerdict{}, err
	}

	// The scheme and host come from the leader's announced API URL —
	// trusted, replicated state, same trust model as the leader-read proxy.
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return cluster.DiskVerdict{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != nethttp.StatusOK {
		// Read a little of the body for the log line, then drop it — the
		// usual case is 503 leader_unavailable when leadership just moved.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return cluster.DiskVerdict{}, fmt.Errorf("disk report: leader returned %d: %s", resp.StatusCode, snippet)
	}

	var verdict diskVerdictWire
	if err := json.NewDecoder(resp.Body).Decode(&verdict); err != nil {
		return cluster.DiskVerdict{}, fmt.Errorf("disk report: decode verdict: %w", err)
	}
	return cluster.DiskVerdict{State: verdict.State, Reason: verdict.Reason, LeaderID: verdict.Leader}, nil
}
