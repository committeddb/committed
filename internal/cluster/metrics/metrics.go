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
	proposals       metric.Int64Counter
	proposeDuration metric.Float64Histogram
	applyIndex      metric.Float64Gauge
	firstIndex      metric.Float64Gauge
	lastIndex       metric.Float64Gauge
	leader          metric.Float64Gauge
	applyDuration   metric.Float64Histogram
	syncDuration    metric.Float64Histogram
	workerRunning   metric.Float64Gauge
	workerReplaces  metric.Int64Counter

	leaderTransitionsObserved metric.Int64Counter
	proposeFailFastUnknown    metric.Int64Counter

	ingestFrozen              metric.Float64Gauge
	ingestRestarts            metric.Int64Counter
	ingestSupervisorGiveups   metric.Int64Counter
	ingestFreezeDrainTimeouts metric.Int64Counter
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

	m.ingestFreezeDrainTimeouts, _ = meter.Int64Counter("committed.ingest.freeze_drain_timeout_total",
		metric.WithDescription("Ingest freeze-path drain hit its timeout with at least one unresolved in-flight bump; supervisor may read a stale position."))

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

// IngestFreezeDrainTimeout increments the freeze-drain timeout counter
// for an ingestable id. Fires when the worker's pre-freeze drain hit
// its deadline with at least one in-flight bump unresolved — the
// supervisor's subsequent storage.Position read may then be stale,
// causing duplicate-row replay on the restart.
func (m *Metrics) IngestFreezeDrainTimeout(id string) {
	m.ingestFreezeDrainTimeouts.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("id", id)))
}
