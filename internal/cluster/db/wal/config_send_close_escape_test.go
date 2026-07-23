package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestRequestReconcile_EscapesOnCloseInsteadOfStranding pins the #1 fix: a
// detached reconcile sender (RequestSyncReconcile / RequestIngestReconcile — run
// from cmd/node startup and the async refreshAfterRestore goroutine) is NOT
// synchronized with db.Close's channel drain, so once the db-layer listener has
// stopped it can be left sending on the unbuffered sync/ingest channel with no
// receiver. Without an escape that goroutine strands for the life of the process.
//
// The sends now select on Storage.closeC, so Close releases them. We reproduce
// the "no receiver" state directly with an unbuffered channel nobody reads, fire
// the reconcile senders, then Close — and require both return promptly. Pre-fix
// (bare `s.sync <- ...`) the senders block forever and this times out.
func TestRequestReconcile_EscapesOnCloseInsteadOfStranding(t *testing.T) {
	dir := t.TempDir()
	// Unbuffered, and we never receive from either: any send blocks until closeC.
	syncCh := make(chan *db.SyncableWithID)
	ingestCh := make(chan *db.IngestableWithID)

	s, err := wal.Open(dir, nil, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)

	done := make(chan struct{}, 2)
	go func() { s.RequestSyncReconcile(); done <- struct{}{} }()
	go func() { s.RequestIngestReconcile(); done <- struct{}{} }()

	// With no receiver on either channel, neither sender may return yet — they
	// are blocked on the send. (Pre-fix they stay blocked forever; that is the bug.)
	select {
	case <-done:
		t.Fatal("a reconcile sender returned before Close — with no receiver it must be blocked on the send")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, s.Close())

	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("reconcile sender did not escape after Close — it stranded on the unbuffered channel")
		}
	}
}
