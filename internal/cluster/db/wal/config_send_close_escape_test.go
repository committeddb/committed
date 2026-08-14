package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestRequestReconcile_NeverBlocksProducers pins the notify-pump contract,
// which superseded the closeC-escape this test originally pinned. History:
// v1 — bare sends could strand a detached reconcile goroutine forever once
// the listener stopped; v2 — sends selected on closeC, so Close released
// them (this test's original shape: blocked-until-Close). v3 (now) — ALL
// notification producers go through the notify pump: a push returns
// IMMEDIATELY regardless of receiver state, because the same send path is
// shared with the raft APPLY LOOP, and the field proved a wedged listener
// could freeze appliedIndex cluster-wide through it (a locked sink table →
// stalled Init → full channel → blocked apply). The pump goroutine owns the
// blocking and the shutdown drop.
func TestRequestReconcile_NeverBlocksProducers(t *testing.T) {
	dir := t.TempDir()
	// Unbuffered, and we never receive from either: the pump absorbs what the
	// producers push; nothing may block the producers themselves.
	syncCh := make(chan *db.SyncableWithID)
	ingestCh := make(chan *db.IngestableWithID)

	s, err := wal.Open(dir, nil, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)

	done := make(chan struct{}, 2)
	go func() { s.RequestSyncReconcile(); done <- struct{}{} }()
	go func() { s.RequestIngestReconcile(); done <- struct{}{} }()

	// Producers return promptly even with NO receiver — the invariant the
	// apply loop depends on.
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a notification producer blocked with no receiver — the pump must absorb pushes so the apply path can never stall on the listener")
		}
	}

	// Close with items still queued in the pump: must not hang or panic (the
	// pump drops on close; boot reconciliation re-emits from durable state).
	require.NoError(t, s.Close())
}
