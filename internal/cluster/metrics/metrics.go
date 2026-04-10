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
