package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds every OTel instrument the committed node exposes.
// A nil *Metrics is safe to use — callers check for nil before calling
// methods, so no instrumentation overhead when metrics are disabled.
type Metrics struct {
	proposals        metric.Int64Counter
	proposeDuration  metric.Float64Histogram
	applyIndex       metric.Float64Gauge
	firstIndex       metric.Float64Gauge
	lastIndex        metric.Float64Gauge
	leader           metric.Float64Gauge
	applyDuration    metric.Float64Histogram
	syncDuration     metric.Float64Histogram
	syncBumpDuration metric.Float64Histogram
	workerRunning    metric.Float64Gauge
	workerReplaces   metric.Int64Counter

	leaderTransitionsObserved metric.Int64Counter
	proposeFailFastUnknown    metric.Int64Counter

	ingestFrozen               metric.Float64Gauge
	ingestRestarts             metric.Int64Counter
	ingestSupervisorGiveups    metric.Int64Counter
	ingestPositionBumpDuration metric.Float64Histogram
	ingestDedupSkipped         metric.Int64Counter

	configBuildErrors metric.Float64Gauge
}

// New creates a Metrics instance from an OTel Meter. The caller
// controls the MeterProvider and exporter; this package only defines
// instruments and domain methods.
func New(meter metric.Meter) *Metrics {
	m := &Metrics{}

	m.proposals, _ = meter.Int64Counter("committed.proposals",
		metric.WithDescription("Total proposals submitted."))

	m.proposeDuration, _ = meter.Float64Histogram("committed.propose.duration",
		metric.WithDescription("Time from db.Propose entry to apply acknowledgement."),
		metric.WithUnit("s"))

	m.applyIndex, _ = meter.Float64Gauge("committed.apply.index",
		metric.WithDescription("Latest raft index applied to local state."))

	m.firstIndex, _ = meter.Float64Gauge("committed.first.index",
		metric.WithDescription("Lowest raft log index still available in storage."))

	m.lastIndex, _ = meter.Float64Gauge("committed.last.index",
		metric.WithDescription("Highest raft log index in storage."))

	m.leader, _ = meter.Float64Gauge("committed.leader",
		metric.WithDescription("1 if this node is the raft leader, 0 otherwise."))

	m.applyDuration, _ = meter.Float64Histogram("committed.apply.duration",
		metric.WithDescription("Time to apply a single committed raft entry."),
		metric.WithUnit("s"))

	m.syncDuration, _ = meter.Float64Histogram("committed.sync.duration",
		metric.WithDescription("Time spent inside a Syncable.Sync call."),
		metric.WithUnit("s"))

	m.syncBumpDuration, _ = meter.Float64Histogram("committed.sync.bump.duration",
		metric.WithDescription("Time from submitting a SyncableIndex bump after a successful Sync until it is durably applied."),
		metric.WithUnit("s"))

	m.workerRunning, _ = meter.Float64Gauge("committed.worker.running",
		metric.WithDescription("1 if a worker goroutine is running for this kind+id, 0 otherwise."))

	m.workerReplaces, _ = meter.Int64Counter("committed.worker.replaces",
		metric.WithDescription("Number of times a worker was replaced via the registry."))

	m.leaderTransitionsObserved, _ = meter.Int64Counter("committed.leader.transitions.observed",
		metric.WithDescription("Raft leader-ID transitions observed by this node's leader-change watcher."))

	m.proposeFailFastUnknown, _ = meter.Int64Counter("committed.propose.fail_fast.unknown",
		metric.WithDescription("In-flight Propose waiters signaled with ErrProposalUnknown by the leader-change watcher."))

	m.ingestFrozen, _ = meter.Float64Gauge("committed.ingest.frozen",
		metric.WithDescription("1 if an ingest worker is parked in the ErrProposalUnknown freeze branch awaiting supervisor restart, 0 otherwise."))

	m.ingestRestarts, _ = meter.Int64Counter("committed.ingest.restart_total",
		metric.WithDescription("Supervisor-triggered ingest worker re-registrations after a freeze."))

	m.ingestSupervisorGiveups, _ = meter.Int64Counter("committed.ingest.supervisor_giveup_total",
		metric.WithDescription("Ingest supervisor give-ups after hitting the consecutive-freeze cap for an id."))

	m.ingestPositionBumpDuration, _ = meter.Float64Histogram("committed.ingest.position.bump.duration",
		metric.WithDescription("Time from submitting an ingestable Position bump after a batch of ingested proposals until it is durably applied."),
		metric.WithUnit("s"))

	m.ingestDedupSkipped, _ = meter.Int64Counter("committed.ingest.dedup_skipped_total",
		metric.WithDescription("Re-emitted ingest proposals skipped before raft because their source sequence was at or below the durable highwater (effectively-once dedup)."))

	m.configBuildErrors, _ = meter.Float64Gauge("committed.config.build_errors",
		metric.WithDescription("Configs (database/ingestable/syncable) persisted on this node but not buildable locally — usually a missing ${VAR} secret. Non-zero means a degraded config, not a down node."))

	return m
}

// ProposalSubmitted increments the proposals counter with the given kind
// (user, config, index, position).
func (m *Metrics) ProposalSubmitted(kind string) {
	m.proposals.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("kind", kind)))
}

// ProposalApplied records the duration from propose to apply.
func (m *Metrics) ProposalApplied(d time.Duration) {
	m.proposeDuration.Record(context.Background(), d.Seconds())
}

// SetLeader sets the leader gauge to 1 or 0.
func (m *Metrics) SetLeader(isLeader bool) {
	v := 0.0
	if isLeader {
		v = 1.0
	}
	m.leader.Record(context.Background(), v)
}

// EntryApplied records the apply duration and updates the apply index gauge.
func (m *Metrics) EntryApplied(index uint64, d time.Duration) {
	m.applyIndex.Record(context.Background(), float64(index))
	m.applyDuration.Record(context.Background(), d.Seconds())
}

// SetIndexRange updates the first and last raft log index gauges.
func (m *Metrics) SetIndexRange(first, last uint64) {
	m.firstIndex.Record(context.Background(), float64(first))
	m.lastIndex.Record(context.Background(), float64(last))
}

// SyncCompleted records the duration of a Syncable.Sync call.
func (m *Metrics) SyncCompleted(id string, d time.Duration) {
	m.syncDuration.Record(context.Background(), d.Seconds(),
		metric.WithAttributes(attribute.String("syncable_id", id)))
}

// SyncBumpCompleted records the round-trip cost of the now-blocking
// SyncableIndex bump that follows a successful Sync — the extra Raft
// round-trip this node pays in exchange for deterministic recovery
// (at most one duplicate on crash instead of a duplicate storm).
// Recorded only on a successful (durable) bump so the histogram
// reflects real round-trip latency, not error-path timing.
func (m *Metrics) SyncBumpCompleted(d time.Duration) {
	m.syncBumpDuration.Record(context.Background(), d.Seconds())
}

// SetWorkerRunning sets the worker running gauge to 1 or 0.
func (m *Metrics) SetWorkerRunning(kind, id string, running bool) {
	v := 0.0
	if running {
		v = 1.0
	}
	m.workerRunning.Record(context.Background(), v,
		metric.WithAttributes(
			attribute.String("kind", kind),
			attribute.String("id", id),
		))
}

// WorkerReplaced increments the worker replace counter.
func (m *Metrics) WorkerReplaced(kind, id string) {
	m.workerReplaces.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("kind", kind),
			attribute.String("id", id),
		))
}

// LeaderTransitionObserved increments the counter recording how often
// this node's leader-change watcher has observed a raft leader-ID
// transition. Serves as a healthy-baseline denominator alongside
// ProposeFailFastUnknown.
func (m *Metrics) LeaderTransitionObserved() {
	m.leaderTransitionsObserved.Add(context.Background(), 1)
}

// ProposeFailFastUnknown increments the counter recording how often an
// in-flight Propose waiter has been signaled with ErrProposalUnknown
// because its stamped leader changed and the grace period expired
// without apply.
func (m *Metrics) ProposeFailFastUnknown() {
	m.proposeFailFastUnknown.Add(context.Background(), 1)
}

// IngestFrozen sets the frozen gauge for an ingestable id. frozen=true
// is emitted by the worker when it enters the ErrProposalUnknown freeze
// branch; frozen=false is emitted by the supervisor after it
// successfully re-registers the ingestable.
func (m *Metrics) IngestFrozen(id string, frozen bool) {
	v := 0.0
	if frozen {
		v = 1.0
	}
	m.ingestFrozen.Record(context.Background(), v,
		metric.WithAttributes(attribute.String("id", id)))
}

// IngestRestart increments the supervisor restart counter for an
// ingestable id. Fires once per successful supervisor-driven
// re-registration.
func (m *Metrics) IngestRestart(id string) {
	m.ingestRestarts.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("id", id)))
}

// IngestSupervisorGiveup increments the supervisor give-up counter for
// an ingestable id. Fires once when the supervisor stops restarting
// after the consecutive-freeze cap.
func (m *Metrics) IngestSupervisorGiveup(id string) {
	m.ingestSupervisorGiveups.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("id", id)))
}

// IngestPositionBumpCompleted records the round-trip cost of the
// now-blocking ingestable Position bump that follows a batch of ingested
// proposals — the extra Raft round-trip this node pays in exchange for a
// durable resume position (bounding duplicate log entries to the
// proposals since the last durable checkpoint instead of an unbounded
// storm on a hard crash). Recorded only on a successful (durable) bump.
func (m *Metrics) IngestPositionBumpCompleted(d time.Duration) {
	m.ingestPositionBumpDuration.Record(context.Background(), d.Seconds())
}

// IngestDedupSkipped counts an ingest proposal dropped before raft
// because its source sequence was at or below the durable highwater — a
// re-emit after a crash/flap that effectively-once dedup elided.
func (m *Metrics) IngestDedupSkipped(id string) {
	m.ingestDedupSkipped.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("id", id)))
}

// SetConfigBuildErrors records how many configs are currently degraded on
// this node (persisted but not buildable locally — usually a missing
// ${VAR} secret). Emitted from the raft Ready loop. A non-zero value is
// an operator signal to fix the environment; the node is still serving.
func (m *Metrics) SetConfigBuildErrors(n int) {
	m.configBuildErrors.Record(context.Background(), float64(n))
}
