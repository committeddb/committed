package sql

import (
	"slices"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// PendingStatus is the status of an ingestable whose durable position is EMPTY —
// nothing has ever durably checkpointed: a just-created ingestable, or one whose
// worker keeps freeze-restarting before its first checkpoint commits. Shared by
// both dialects so the pending shape has one source of truth.
//
// This state must never render as streaming/complete: the durable state space is
// three-valued (never-progressed / mid-snapshot / completed-streaming), and
// before this helper existed both dialects collapsed it to two — an empty
// position decoded to a nil snapshot-progress, which SnapshotTableStatus (below)
// reads as "snapshot done, all tables complete" and the phase branch reads as
// "streaming". That false green (phase "streaming", complete true, for an
// ingestable that had ingested NOTHING) defeated three rounds of incident
// forensics. Lag is left nil (the slot/binlog reader may not even exist yet) so
// CaughtUp can never be true.
func PendingStatus(config *Config) cluster.IngestableStatus {
	tables := make([]cluster.TableSnapshotStatus, 0, len(config.Tables))
	for _, t := range config.Tables {
		st := cluster.TableSnapshotStatus{Table: t}
		if spec := config.SpecForTable(t); spec != nil && spec.Type != nil {
			st.Topic = spec.Type.ID
		}
		tables = append(tables, st) // Complete deliberately false: nothing has run
	}
	return cluster.IngestableStatus{
		Phase:            "pending",
		SnapshotProgress: tables,
	}
}

// SnapshotTableStatus reports each configured table's place in the initial
// snapshot, shared by both dialects since the snapshot-progress proto is shared.
// Callers must route an EMPTY position to PendingStatus first — this helper's
// progress==nil arm is only correct for a NON-empty (streaming) position.
//
// During the snapshot phase (progress != nil) it reflects the live cursor: a
// table in CompletedTables reads Complete, the rest carry their keyset cursor
// from LastPkByTable. Once the snapshot is done (progress == nil — the streaming
// phase no longer checkpoints snapshot progress) every configured table reads
// Complete, since reaching streaming means the snapshot covered them all.
func SnapshotTableStatus(config *Config, progress *dialectpb.SnapshotProgress) []cluster.TableSnapshotStatus {
	out := make([]cluster.TableSnapshotStatus, 0, len(config.Tables))
	for _, t := range config.Tables {
		st := cluster.TableSnapshotStatus{Table: t}
		// Tag each table with the topic it feeds so per-topic snapshot progress is
		// readable off the flat list (one topic for the flat form; N for
		// [[sql.topics]]). SpecForTable resolves the raw config entry directly.
		if spec := config.SpecForTable(t); spec != nil && spec.Type != nil {
			st.Topic = spec.Type.ID
		}
		// A table is complete when there's no in-progress snapshot, or it's in the
		// completed set. The keyset cursor (PK) is deliberately not surfaced — it is
		// often source PII (see TableSnapshotStatus).
		if progress == nil || slices.Contains(progress.CompletedTables, t) {
			st.Complete = true
		}
		out = append(out, st)
	}
	return out
}
