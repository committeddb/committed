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

	syncErrors          metric.Int64Counter
	syncLastErrorTime   metric.Float64Gauge
	syncStuck           metric.Float64Gauge
	ingestErrors        metric.Int64Counter
	ingestLastErrorTime metric.Float64Gauge

	leaderTransitionsObserved metric.Int64Counter
	proposeFailFastUnknown    metric.Int64Counter

	readIndexDuration metric.Float64Histogram

	ingestFrozen               metric.Float64Gauge
	ingestRestarts             metric.Int64Counter
	ingestSupervisorGiveups    metric.Int64Counter
	ingestPositionBumpDuration metric.Float64Histogram
	ingestDedupSkipped         metric.Int64Counter

	typeMigrationErrors   metric.Int64Counter
	typeMigrationDuration metric.Float64Histogram

	configBuildErrors metric.Float64Gauge

	walCorruptEntries metric.Int64Counter

	diskFreeBytes   metric.Float64Gauge
	diskFreePercent metric.Float64Gauge
	diskState       metric.Float64Gauge

	diskClusterState        metric.Float64Gauge
	writeAdmitted           metric.Float64Gauge
	writeAdmissionReason    metric.Float64Gauge
	diskLeadershipTransfers metric.Int64Counter
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

	m.syncErrors, _ = meter.Int64Counter("committed.sync.errors",
		metric.WithDescription("Sync errors by syncable_id and kind (permanent|transient). A permanent error dead-letters and skips the proposal; transient errors retry."))

	m.syncLastErrorTime, _ = meter.Float64Gauge("committed.sync.last_error.timestamp",
		metric.WithDescription("Unix time (seconds) of the most recent sync error for a syncable_id. Pair with sync_errors_total to spot a worker stuck in a retry loop."),
		metric.WithUnit("s"))

	m.syncStuck, _ = meter.Float64Gauge("committed.sync.stuck",
		metric.WithDescription("1 if a syncable's worker has been blocked retrying a transient error past the stuck threshold (an operator can skip it via POST /syncable/{id}/deadletter/), 0 otherwise. Alert on this."))

	m.ingestErrors, _ = meter.Int64Counter("committed.ingest.errors",
		metric.WithDescription("Ingest errors by ingestable_id and kind (propose|position). A failure to commit an ingested proposal or its position checkpoint."))

	m.ingestLastErrorTime, _ = meter.Float64Gauge("committed.ingest.last_error.timestamp",
		metric.WithDescription("Unix time (seconds) of the most recent ingest error for an ingestable_id."),
		metric.WithUnit("s"))

	m.leaderTransitionsObserved, _ = meter.Int64Counter("committed.leader.transitions.observed",
		metric.WithDescription("Raft leader-ID transitions observed by this node's leader-change watcher."))

	m.proposeFailFastUnknown, _ = meter.Int64Counter("committed.propose.fail_fast.unknown",
		metric.WithDescription("In-flight Propose waiters signaled with ErrProposalUnknown by the leader-change watcher."))

	m.readIndexDuration, _ = meter.Float64Histogram("committed.read_index.duration",
		metric.WithDescription("Time for a linearizable read to complete: the ReadIndex quorum round-trip plus the wait for local AppliedIndex to catch up."),
		metric.WithUnit("s"))

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

	m.typeMigrationErrors, _ = meter.Int64Counter("committed.type.migration.errors",
		metric.WithDescription("Runtime type-migration failures by type_id and the failing chain step (from_version -> to_version). Each counted failure dead-letters the proposal for the syncable that hit it; query GET /type/{id}/migration-errors for the records."))

	m.typeMigrationDuration, _ = meter.Float64Histogram("committed.type.migration.duration",
		metric.WithDescription("Time to run an entity through its type-migration chain (all steps from the stamped version to current). Recorded only on success, so the histogram reflects real transform cost."),
		metric.WithUnit("s"))

	m.configBuildErrors, _ = meter.Float64Gauge("committed.config.build_errors",
		metric.WithDescription("Configs (database/ingestable/syncable) persisted on this node but not buildable locally — usually a missing ${VAR} secret. Non-zero means a degraded config, not a down node."))

	m.walCorruptEntries, _ = meter.Int64Counter("committed.wal.corrupt_entries",
		metric.WithDescription("WAL entries that failed CRC32C checksum verification on read, by log (entry_log|event_log|state_log). Any non-zero value means on-disk corruption was detected (bit rot, torn write, filesystem damage); the node will fatal-exit. Alert on this and rebuild from a healthy peer per docs/operations/rebuild.md."))

	m.diskFreeBytes, _ = meter.Float64Gauge("committed.disk.free_bytes",
		metric.WithDescription("Free bytes on the filesystem backing the data directory, sampled by the disk-usage watcher."),
		metric.WithUnit("By"))

	m.diskFreePercent, _ = meter.Float64Gauge("committed.disk.free_percent",
		metric.WithDescription("Free space on the data directory's filesystem as a percent of total, sampled by the disk-usage watcher."))

	m.diskState, _ = meter.Float64Gauge("committed.disk.state",
		metric.WithDescription("Disk-pressure level of the data directory as mutually-exclusive gauges (level=ok|warn|critical|full); exactly one is 1, the rest 0. At critical, user proposals are rejected with 507; at full, the node is read-only. Alert on critical/full."))

	m.diskClusterState, _ = meter.Float64Gauge("committed.disk.cluster_state",
		metric.WithDescription("Cluster-effective disk-pressure level this node's write-admission gate is enforcing, as mutually-exclusive gauges (level=ok|warn|critical|full). Computed by the leader from all voter disk states: critical/full here means writes are being rejected cluster-wide. Alert on critical/full."))

	m.writeAdmitted, _ = meter.Float64Gauge("committed.write.admitted",
		metric.WithDescription("Whether this node's gate currently admits user-data writes (1) or rejects them with 507 (0). Alert on 0; diagnose with committed.write.admission_reason and committed.disk.cluster_state."))

	m.writeAdmissionReason, _ = meter.Float64Gauge("committed.write.admission_reason",
		metric.WithDescription("Dominant cause behind committed.write.admitted, as mutually-exclusive gauges (reason=ok|leader_disk|quorum_at_risk|cluster_reject|local_fallback); exactly one is 1. quorum_at_risk is the cluster-outage early warning: a quorum of voters is low on disk — expand disk or replace nodes before quorum is lost."))

	m.diskLeadershipTransfers, _ = meter.Int64Counter("committed.disk.leadership_transfers",
		metric.WithDescription("Leadership transfers triggered because the leader's own disk reached critical/full while a healthy voter existed. Each transfer converts a cluster-wide write freeze into a single constrained follower."))

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

// SyncError counts one sync error for a syncable and stamps the
// last-error timestamp gauge. kind is "permanent" (the proposal was
// dead-lettered and skipped) or "transient" (the worker will retry).
// Node-local observability — emitted from the sync worker, which runs on
// one node at a time; alert on the counter, dashboard the timestamp.
func (m *Metrics) SyncError(id, kind string) {
	m.syncErrors.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("syncable_id", id),
			attribute.String("kind", kind),
		))
	m.syncLastErrorTime.Record(context.Background(), float64(time.Now().Unix()),
		metric.WithAttributes(attribute.String("syncable_id", id)))
}

// MigrationError counts one runtime type-migration failure for a type,
// attributed to the failing chain step (the toVersion program is the one
// that errored). Emitted from the sync worker's dead-letter path, so a
// batch failure isolated down to one bad entity counts once.
func (m *Metrics) MigrationError(typeID string, fromVersion, toVersion int) {
	m.typeMigrationErrors.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("type_id", typeID),
			attribute.Int("from_version", fromVersion),
			attribute.Int("to_version", toVersion),
		))
}

// MigrationCompleted records the time an entity spent in its type-migration
// chain (every step from the stamped version up to current). Recorded only
// on success — failures are counted by MigrationError instead.
func (m *Metrics) MigrationCompleted(typeID string, d time.Duration) {
	m.typeMigrationDuration.Record(context.Background(), d.Seconds(),
		metric.WithAttributes(attribute.String("type_id", typeID)))
}

// IngestError counts one ingest error for an ingestable and stamps the
// last-error timestamp gauge. kind is "propose" (a failure to commit an
// ingested proposal) or "position" (a failure to commit a position
// checkpoint). Context-cancellation on shutdown is not counted — the
// caller guards on ctx.
func (m *Metrics) IngestError(id, kind string) {
	m.ingestErrors.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("ingestable_id", id),
			attribute.String("kind", kind),
		))
	m.ingestLastErrorTime.Record(context.Background(), float64(time.Now().Unix()),
		metric.WithAttributes(attribute.String("ingestable_id", id)))
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

// ReadIndexCompleted records the end-to-end latency of a successful
// linearizable read — the ReadIndex quorum confirmation round-trip plus the
// wait for local AppliedIndex to reach the confirmed index. Recorded only on
// success, so the histogram reflects real round-trip cost rather than
// timeout/partition error-path timing.
func (m *Metrics) ReadIndexCompleted(d time.Duration) {
	m.readIndexDuration.Record(context.Background(), d.Seconds())
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

// SetSyncStuck sets the stuck gauge for a syncable id (1 = blocked past the
// stuck threshold, 0 = not). Driven by the worker's stuck tracker when it
// publishes / clears the replicated SyncableStuck record.
func (m *Metrics) SetSyncStuck(id string, stuck bool) {
	v := 0.0
	if stuck {
		v = 1.0
	}
	m.syncStuck.Record(context.Background(), v,
		metric.WithAttributes(attribute.String("syncable_id", id)))
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

// WalCorruptEntry counts one WAL entry that failed checksum verification on
// read, attributed to which log it came from ("entry_log", "event_log", or
// "state_log"). Emitted from the storage read path the moment corruption is
// detected, before the error propagates to the fatal-exit.
func (m *Metrics) WalCorruptEntry(log string) {
	m.walCorruptEntries.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("log", log)))
}

// SetDiskFree records the most recent free-space sample for the data
// directory's filesystem: absolute free bytes and free percent of total.
// Emitted from the disk-usage watcher on every poll.
func (m *Metrics) SetDiskFree(freeBytes uint64, freePercent float64) {
	m.diskFreeBytes.Record(context.Background(), float64(freeBytes))
	m.diskFreePercent.Record(context.Background(), freePercent)
}

// SetDiskState publishes the current disk-pressure level as a set of
// mutually-exclusive gauges: the active level records 1, every other level
// records 0, so a dashboard/alert can match committed_disk_state{level="full"}
// without worrying about a stale 1 lingering on a level we've left. active is
// one of "ok", "warn", "critical", "full".
func (m *Metrics) SetDiskState(active string) {
	for _, level := range [...]string{"ok", "warn", "critical", "full"} {
		v := 0.0
		if level == active {
			v = 1.0
		}
		m.diskState.Record(context.Background(), v,
			metric.WithAttributes(attribute.String("level", level)))
	}
}

// SetDiskClusterState publishes the cluster-effective disk-pressure level this
// node's write-admission gate is enforcing, in the same mutually-exclusive
// gauge shape as SetDiskState. Emitted on every admission update — the leader
// recomputing the verdict, a follower receiving one, or either falling back
// to the node-local decision.
func (m *Metrics) SetDiskClusterState(active string) {
	for _, level := range [...]string{"ok", "warn", "critical", "full"} {
		v := 0.0
		if level == active {
			v = 1.0
		}
		m.diskClusterState.Record(context.Background(), v,
			metric.WithAttributes(attribute.String("level", level)))
	}
}

// SetWriteAdmission publishes whether this node's gate currently admits
// user-data writes (committed.write.admitted, 1/0) and the dominant cause
// (committed.write.admission_reason as mutually-exclusive per-reason gauges,
// same shape as SetDiskState, so an alert can match a reason without a stale
// 1 lingering after the cause changes). reason is one of the bounded codes
// ok | leader_disk | quorum_at_risk | cluster_reject | local_fallback —
// cluster_reject is a follower enforcing a leader-computed rejection (the
// leader_disk/quorum_at_risk breakdown lives on the leader's own gauge);
// local_fallback means no fresh cluster verdict was available and the
// node-local Phase 1 decision is in force.
func (m *Metrics) SetWriteAdmission(admitted bool, reason string) {
	v := 0.0
	if admitted {
		v = 1.0
	}
	m.writeAdmitted.Record(context.Background(), v)

	for _, code := range [...]string{"ok", "leader_disk", "quorum_at_risk", "cluster_reject", "local_fallback"} {
		rv := 0.0
		if code == reason {
			rv = 1.0
		}
		m.writeAdmissionReason.Record(context.Background(), rv,
			metric.WithAttributes(attribute.String("reason", code)))
	}
}

// DiskLeadershipTransfer counts a disk-pressure leadership transfer: the
// leader's own disk reached critical/full and it handed leadership to a
// confirmed-healthy voter.
func (m *Metrics) DiskLeadershipTransfer() {
	m.diskLeadershipTransfers.Add(context.Background(), 1)
}
