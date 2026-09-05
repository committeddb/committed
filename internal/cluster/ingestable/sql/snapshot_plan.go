package sql

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// SnapshotKind is why a snapshot pass runs — the four shared decisions every
// dialect's lifecycle makes, each with its own epoch rule and progress shape.
type SnapshotKind int

const (
	// SnapshotCold is a fresh full snapshot with no checkpoint to resume: a
	// first run, or a same-topic recreate whose cleared position reset the
	// epoch while the sink still holds rows up to the topic highwater.
	SnapshotCold SnapshotKind = iota
	// SnapshotResume continues a mid-snapshot checkpoint (full or partial).
	SnapshotResume
	// SnapshotBackfill enumerates only tables a config change added, with
	// every already-snapshotted sibling pre-seeded complete.
	SnapshotBackfill
	// SnapshotGap is the loud automatic recovery from a source gap — a purged
	// binlog, a recreated replication slot, change-tracking retention passed
	// the consumed version — a full re-snapshot at a bumped epoch.
	SnapshotGap
)

// SnapshotPlan is what PlanSnapshot decides: the live progress cursor to run
// with, the generation to stamp, and whether completion closes with
// refresh-boundary markers.
type SnapshotPlan struct {
	Progress *dialectpb.SnapshotProgress
	Epoch    uint64
	Marker   bool
}

// PlanSnapshot resolves the shared snapshot decision. checkpointEpoch is the
// epoch decoded from the position (0 when none or cleared), floor the
// delete-surviving per-topic highwater the sink still carries.
//
//   - Cold stamps strictly ABOVE the highwater (RefreshSnapshotEpoch) so the
//     closing marker's sweep reconciles rows this upsert-only enumeration
//     cannot re-emit; a genuine first snapshot starts at 1.
//   - Resume keeps the in-progress epoch its checkpoint carries — its
//     closing marker has not committed, so the topic highwater does not yet
//     reflect this refresh — and closes with a marker unless the checkpoint
//     is a partial backfill.
//   - Backfill stamps at the current epoch, floored so rows never land below
//     a generation already on the sink, pre-seeds every sibling complete,
//     and emits NO marker: a topic-scoped sweep would delete the sibling
//     rows the backfill deliberately does not re-emit.
//   - Gap bumps above everything seen so the closing markers sweep rows
//     deleted at the source inside the lost window.
func PlanSnapshot(kind SnapshotKind, resume *dialectpb.SnapshotProgress, checkpointEpoch, floor uint64, tables, added []string) SnapshotPlan {
	switch kind {
	case SnapshotResume:
		p := NewSnapshotProgress(resume)
		return SnapshotPlan{Progress: p, Epoch: max(checkpointEpoch, 1), Marker: !p.PartialBackfill}
	case SnapshotBackfill:
		p := NewSnapshotProgress(nil)
		p.PartialBackfill = true
		addedSet := make(map[string]bool, len(added))
		for _, t := range added {
			addedSet[t] = true
		}
		for _, t := range tables {
			if !addedSet[t] {
				p.CompletedTables = append(p.CompletedTables, t)
			}
		}
		return SnapshotPlan{Progress: p, Epoch: max(checkpointEpoch, floor, 1), Marker: false}
	case SnapshotGap:
		return SnapshotPlan{Progress: NewSnapshotProgress(nil), Epoch: max(checkpointEpoch, floor, 1) + 1, Marker: true}
	default:
		return SnapshotPlan{Progress: NewSnapshotProgress(nil), Epoch: RefreshSnapshotEpoch(checkpointEpoch, floor), Marker: true}
	}
}

// FloorEpoch is the generation a stream session stamps: never below the
// sink's highwater, never below 1.
func FloorEpoch(epoch, floor uint64) uint64 {
	return max(epoch, floor, 1)
}

// CompleteSnapshot closes a snapshot pass: when the plan carries a marker,
// one refresh-boundary marker per topic at epoch (one marker per proposal
// keeps each proposal homogeneous — one topic — matching the flush path; the
// flat single-topic form emits exactly one), then the completion checkpoint
// the dialect encoded (progress cleared, its registry recording every
// configured table) so a restart streams instead of re-snapshotting.
func CompleteSnapshot(ctx context.Context, config *Config, marker bool, epoch uint64, checkpoint []byte, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	if marker {
		config.EnsureTopics()
		for i := range config.Topics {
			spec := &config.Topics[i]
			if spec.Type == nil {
				continue
			}
			m := cluster.NewRefreshBoundaryEntity(spec.Type, epoch)
			select {
			case pr <- &cluster.Proposal{Entities: []*cluster.Entity{m}}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	select {
	case po <- checkpoint:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
