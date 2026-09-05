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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
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
// node then enforces the verdict at its own propose gate (checkWritable),
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

// diskAdmissionDeps is the subsystem's entire view of the engine plus its
// tunables, wired by db.New. The engine views are function fields — the
// subsystem never holds *DB — so the field list is the exact coupling
// surface, and admission logic is unit-testable with plain stubs.
type diskAdmissionDeps struct {
	selfID             func() uint64
	leaderID           func() uint64
	isLeader           func() bool
	memberAPIURL       func(id uint64) (string, bool)
	voters             func() map[uint64]struct{}
	replicationMatch   func() map[uint64]uint64
	transferLeadership func(transferee uint64)

	// reportInterval is the coordinator cadence (0 = default; negative
	// leaves the loop unstarted — db.New gates run() on it). send, when
	// nil, is built from reportClient/reportToken.
	reportInterval   time.Duration
	transferCooldown time.Duration
	send             diskReportSender
	reportClient     *nethttp.Client
	reportToken      string

	ctx     context.Context
	logger  *zap.Logger
	metrics *metrics.Metrics
}

// diskAdmission is the write-admission subsystem: the node-local disk gate
// (Phase 1) plus the cluster-aware verdict machinery documented above. It
// owns all admission state; the engine consults it at the propose gate
// (checkWritable), feeds it local disk-state changes (setLocalState), and
// exposes it over HTTP through DB's delegators (DiskState, DiskAdmission,
// ReportDisk).
type diskAdmission struct {
	diskAdmissionDeps

	// local is the current node-local disk-pressure level (a diskState
	// value), published by the disk-usage watcher via setLocalState and
	// read by the propose gate. Stored as an int32 so it can be updated
	// atomically without a lock on the hot propose path. Stays diskOK when
	// no watcher is configured, so the gate is a no-op.
	local atomic.Int32

	// verdict is the cached cluster write-admission verdict the propose
	// gate enforces (atomic pointer: the gate reads it lock-free on the hot
	// path; nil or stale falls back to the node-local state). reports is
	// the leader-side map of member disk reports, guarded by reportsMu.
	// nudgeC wakes the coordinator loop early on a local disk-state change.
	// lastTransfer rate-limits disk-pressure leadership transfers; it is
	// touched only from coordinator cycles.
	verdict      atomic.Pointer[diskVerdictState]
	reportsMu    sync.Mutex
	reports      map[uint64]diskReport
	nudgeC       chan struct{}
	lastTransfer time.Time

	// pickTransferTargetFn is the transfer-target seam, defaulted to the
	// subsystem's own report-driven picker; tests override it to drive the
	// transfer decision without a live voter set.
	pickTransferTargetFn func(now time.Time) uint64
}

// newDiskAdmission builds the subsystem with production defaults applied:
// report interval, transfer cooldown, the HTTP report sender, and the
// report-driven transfer-target picker.
func newDiskAdmission(deps diskAdmissionDeps) *diskAdmission {
	a := &diskAdmission{
		diskAdmissionDeps: deps,
		reports:           make(map[uint64]diskReport),
		nudgeC:            make(chan struct{}, 1),
	}
	if a.reportInterval == 0 {
		a.reportInterval = DefaultDiskReportInterval
	}
	if a.transferCooldown == 0 {
		a.transferCooldown = defaultDiskTransferCooldown
	}
	if a.send == nil {
		a.send = newHTTPDiskReportSender(a.reportClient, a.reportToken)
	}
	a.pickTransferTargetFn = a.pickTransferTarget
	return a
}

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

// localState is the node-local disk-pressure level as last published by the
// disk-usage watcher.
func (a *diskAdmission) localState() diskState {
	return diskState(a.local.Load())
}

// setLocalState publishes a new node-local disk-pressure level for the
// propose gate to read. On the leader it then synchronously recomputes the
// cluster admission verdict (so the gate never enforces a verdict older than
// the state just published); on a follower it nudges the coordinator to
// report the transition to the leader right away. Called from db.onDiskState
// — the watcher's callback — and tests via SetDiskStateForTest.
func (a *diskAdmission) setLocalState(s diskState) {
	a.local.Store(int32(s))
	if a.isLeader() {
		a.recomputeVerdict(time.Now())
		return
	}
	a.nudge()
}

// verdictTTL is the freshness window for the cached verdict and for member
// reports. Derived from the report interval so shortening the interval
// (tests, aggressive ops) tightens staleness proportionally.
func (a *diskAdmission) verdictTTL() time.Duration {
	interval := a.reportInterval
	if interval <= 0 {
		interval = DefaultDiskReportInterval
	}
	return diskVerdictTTLFactor * interval
}

// admissionState returns the disk-pressure level the propose gate should
// enforce right now and, when that decision comes from a fresh cluster
// verdict, the verdict itself (nil means node-local fallback). The cluster
// verdict — when fresh — dominates the local state in BOTH directions: a
// full follower admits writes a healthy cluster can serve (they commit on
// the leader's quorum regardless of this node's disk), and a healthy node
// rejects writes the cluster has deemed unsafe (closing the follower→leader
// forwarding bypass).
func (a *diskAdmission) admissionState(now time.Time) (diskState, *diskVerdictState) {
	if v := a.verdict.Load(); v != nil && now.Sub(v.at) <= a.verdictTTL() {
		return v.state, v
	}
	return a.localState(), nil
}

// checkWritable returns the typed error a proposal of the given kind should
// be rejected with under the current admission decision, or nil if it may
// proceed. The hot path is an atomic pointer load + comparison; with no
// watcher and no verdict it always returns nil.
func (a *diskAdmission) checkWritable(kind string) error {
	state, _ := a.admissionState(time.Now())
	return diskRejection(state, kind)
}

// status is the write-admission decision this node's propose gate is
// applying right now — the fresh cluster verdict when one is held, or the
// node-local fallback. Powers GET /node/status via db.DiskAdmission.
func (a *diskAdmission) status() cluster.DiskAdmissionStatus {
	state, v := a.admissionState(time.Now())
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

// report records member nodeID's disk state and returns the freshly
// recomputed cluster verdict. Powers POST /v1/node/disk-report via
// db.ReportDisk. Only the leader aggregates — on any other node it returns
// cluster.ErrNotLeader so the reporter re-resolves the leader on its next
// cycle.
func (a *diskAdmission) report(nodeID uint64, state string) (cluster.DiskVerdict, error) {
	s, ok := parseDiskState(state)
	if !ok {
		return cluster.DiskVerdict{}, fmt.Errorf("unknown disk state %q", state)
	}
	if !a.isLeader() {
		return cluster.DiskVerdict{LeaderID: a.leaderID()}, cluster.ErrNotLeader
	}

	now := time.Now()
	a.reportsMu.Lock()
	a.reports[nodeID] = diskReport{state: s, at: now}
	a.reportsMu.Unlock()

	v := a.recomputeVerdict(now)
	return cluster.DiskVerdict{State: v.state.String(), Reason: v.reason, LeaderID: v.leaderID}, nil
}

// recomputeVerdict computes, publishes, and returns the cluster verdict from
// this node's (the leader's) own disk state plus the collected member
// reports. Called on the coordinator tick, on every report received, and on
// a local disk-state change — so the leader's cached verdict is never older
// than one report interval.
func (a *diskAdmission) recomputeVerdict(now time.Time) *diskVerdictState {
	v := a.computeVerdict(now)
	a.verdict.Store(v)
	a.publishMetrics()
	return v
}

// computeVerdict gathers this node's inputs — its own state, the voter set,
// the collected reports — and runs them through the quorum math.
func (a *diskAdmission) computeVerdict(now time.Time) *diskVerdictState {
	local := a.localState()
	voters := a.voters()

	a.reportsMu.Lock()
	defer a.reportsMu.Unlock()
	return diskVerdictFrom(a.selfID(), local, voters, a.reports, now, a.verdictTTL())
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
// entries are pruned from reports in place — the caller holds reportsMu.
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

// publishMetrics records this node's current effective admission view: the
// cluster-level disk state gauge plus the writes-admitted gauge with its
// bounded reason code ("ok", "leader_disk", "quorum_at_risk", or
// "local_fallback" when no fresh verdict is held).
func (a *diskAdmission) publishMetrics() {
	if a.metrics == nil {
		return
	}
	state, v := a.admissionState(time.Now())
	code := "local_fallback"
	if v != nil {
		code = v.reasonCode
	}
	a.metrics.SetDiskClusterState(state.String())
	a.metrics.SetWriteAdmission(state < diskCritical, code)
}

// nudge wakes the coordinator ahead of its next tick — called on a local
// disk-state change so a follower reports the transition (and picks up the
// resulting verdict) immediately instead of up to one interval late.
// Non-blocking: a pending nudge is enough.
func (a *diskAdmission) nudge() {
	select {
	case a.nudgeC <- struct{}{}:
	default:
	}
}

// run is the per-node admission loop, started from db.New and stopped by
// db.Close via the wired ctx. Each cycle: the leader recomputes the verdict
// and considers transferring leadership away from its own disk pressure; a
// follower reports its disk state to the leader's announced API URL and
// caches the verdict the response carries. Disabled (never started) when the
// report interval is configured negative.
func (a *diskAdmission) run() {
	ticker := time.NewTicker(a.reportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-a.nudgeC:
		case <-a.ctx.Done():
			return
		}
		a.coordinate(time.Now())
	}
}

// coordinate runs one coordinator cycle. Split from the loop so a local
// disk-state change (setLocalState) can run a synchronous cycle on the
// leader — keeping the verdict the gate reads consistent with the state the
// watcher just published, with no window where a test (or a burst of
// proposals) observes the old verdict.
func (a *diskAdmission) coordinate(now time.Time) {
	if a.isLeader() {
		v := a.recomputeVerdict(now)
		a.maybeTransferLeadership(now, v)
		return
	}
	a.reportToLeader(now)
	a.publishMetrics()
}

// reportToLeader performs the follower half of one cycle: resolve the
// leader's announced API URL, POST this node's disk state, and cache the
// verdict from the response. Any failure leaves the cached verdict to age
// out, after which the gate falls back to the node-local decision — so the
// failure modes (no leader, no announced URL, leader unreachable, leadership
// just moved) all degrade to Phase 1 behavior. Failures log at Debug: a
// persistent one shows up as the local_fallback admission metric, not log
// spam every interval.
func (a *diskAdmission) reportToLeader(now time.Time) {
	leaderID := a.leaderID()
	if leaderID == 0 || leaderID == a.selfID() {
		return
	}
	leaderURL, ok := a.memberAPIURL(leaderID)
	if !ok || leaderURL == "" {
		return
	}

	state := a.localState()
	ctx, cancel := context.WithTimeout(a.ctx, defaultDiskReportTimeout)
	defer cancel()
	verdict, err := a.send(ctx, leaderURL, a.selfID(), state.String())
	if err != nil {
		a.logger.Debug("disk report to leader failed",
			zap.Uint64("leader", leaderID), zap.Error(err))
		return
	}

	a.applyVerdict(verdict, now)
}

// applyVerdict caches a verdict the leader returned on a report round trip,
// making it the decision this node's propose gate enforces for the next TTL
// window. Logs only on a state change so the steady-state report loop is
// silent.
func (a *diskAdmission) applyVerdict(verdict cluster.DiskVerdict, now time.Time) {
	s, ok := parseDiskState(verdict.State)
	if !ok {
		a.logger.Debug("disk report: leader returned unknown verdict state",
			zap.String("state", verdict.State))
		return
	}
	code := "ok"
	if s >= diskCritical {
		code = "cluster_reject"
	}
	prev := a.verdict.Load()
	a.verdict.Store(&diskVerdictState{
		state:      s,
		reason:     verdict.Reason,
		reasonCode: code,
		leaderID:   verdict.LeaderID,
		at:         now,
	})
	if prev == nil || prev.state != s {
		a.logger.Info("cluster write-admission verdict changed",
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
func (a *diskAdmission) maybeTransferLeadership(now time.Time, v *diskVerdictState) {
	local := a.localState()
	if local < diskCritical {
		return
	}
	if now.Sub(a.lastTransfer) < a.transferCooldown {
		return
	}
	target := a.pickTransferTargetFn(now)
	if target == 0 {
		return
	}

	verdict := local
	if v != nil {
		verdict = v.state
	}
	a.logger.Warn("leader disk constrained; transferring leadership to a healthy voter",
		zap.String("disk_state", local.String()),
		zap.Uint64("target", target),
		zap.String("verdict", verdict.String()))
	if a.metrics != nil {
		a.metrics.DiskLeadershipTransfer()
	}
	a.lastTransfer = now
	a.transferLeadership(target)
}

// pickTransferTarget gathers the leader-side inputs (voter set, replication
// progress, collected reports) for pickDiskTransferTarget. It is the default
// value of pickTransferTargetFn.
func (a *diskAdmission) pickTransferTarget(now time.Time) uint64 {
	voters := a.voters()
	match := a.replicationMatch()

	a.reportsMu.Lock()
	defer a.reportsMu.Unlock()
	return pickDiskTransferTarget(a.selfID(), voters, a.reports, match, now, a.verdictTTL())
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

// DiskState returns this node's own disk-pressure level as last sampled by
// the local disk watcher. Powers GET /node/status.
func (db *DB) DiskState() string {
	return db.disk.localState().String()
}

// DiskAdmission returns the write-admission decision this node's propose
// gate is applying right now — the fresh cluster verdict when one is held,
// or the node-local fallback. Powers GET /node/status.
func (db *DB) DiskAdmission() cluster.DiskAdmissionStatus {
	return db.disk.status()
}

// ReportDisk records member nodeID's disk state and returns the freshly
// recomputed cluster verdict. Powers POST /v1/node/disk-report.
func (db *DB) ReportDisk(nodeID uint64, state string) (cluster.DiskVerdict, error) {
	return db.disk.report(nodeID, state)
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
// handler. (db can't import its own transport subpackage,
// internal/cluster/db/http: that would be an import cycle once the handlers
// hold *db.DB.)
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
