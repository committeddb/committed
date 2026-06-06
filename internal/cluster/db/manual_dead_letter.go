package db

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

// DeadLetterStuckSyncable asks the worker for id to dead-letter the proposal
// it is currently blocked on and advance past it — the operator's "this one
// will never apply, skip it" lever for a syncable wedged on a transient error
// (transient errors retry forever, so it stalls rather than losing data until
// a human steps in).
//
// Node-agnostic: it reads the replicated SyncableStuck record (any node has
// it) to find the blocked index, then proposes a SyncableSkipRequest through
// Raft (which forwards to the leader, where the worker runs). The worker
// honors it on its next retry — single-skip or batch-isolate — and writes the
// "manual" dead letter. Returns the targeted raft index, or ErrSyncNotStuck
// if the syncable isn't currently blocked (so any node can answer 409).
func (db *DB) DeadLetterStuckSyncable(ctx context.Context, id string) (uint64, error) {
	stuck, ok, err := db.storage.SyncableStuck(id)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, cluster.ErrSyncNotStuck
	}
	if err := db.proposeSyncableSkipRequest(ctx, &cluster.SyncableSkipRequest{ID: id, Index: stuck.Index}); err != nil {
		return 0, err
	}
	return stuck.Index, nil
}

// SyncableStuck reports whether the syncable is currently blocked and, if so,
// on which raft index (plus when and the last error). Backed by replicated
// state, so any node answers identically — powers GET /syncable/{id}/status.
func (db *DB) SyncableStuck(id string) (cluster.SyncableStuck, bool, error) {
	return db.storage.SyncableStuck(id)
}

// proposeSyncableStuck publishes (or refreshes) the worker's "blocked on
// index N" record. BLOCKS until applied; best-effort from the worker's view
// (a failure just means the stall isn't visible cluster-wide yet, and the
// next iteration retries).
func (db *DB) proposeSyncableStuck(ctx context.Context, s *cluster.SyncableStuck) error {
	e, err := cluster.NewUpsertSyncableStuckEntity(s)
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}

// proposeDeleteSyncableStuck clears the worker's stuck record on progress.
func (db *DB) proposeDeleteSyncableStuck(ctx context.Context, id string) error {
	e := cluster.NewDeleteSyncableStuckEntity(id)
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}

// proposeSyncableSkipRequest records an operator's request to skip what the
// syncable is blocked on. Proposed by the endpoint, from any node.
func (db *DB) proposeSyncableSkipRequest(ctx context.Context, r *cluster.SyncableSkipRequest) error {
	e, err := cluster.NewUpsertSyncableSkipRequestEntity(r)
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}

// proposeDeleteSyncableSkipRequest clears a skip request once the worker has
// honored it (or found it stale).
func (db *DB) proposeDeleteSyncableSkipRequest(ctx context.Context, id string) error {
	e := cluster.NewDeleteSyncableSkipRequestEntity(id)
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}
