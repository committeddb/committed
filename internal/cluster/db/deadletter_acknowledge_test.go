package db_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The acknowledge verb end to end against a real wal-backed DB: a
// dead-lettered proposal is marked resolved out-of-band through Raft, the
// completeness count goes green without touching any sink, the record stays
// listable as the audit trail, and the operation is idempotent. The
// superseded case this exists for: replay would REGRESS a sink row a later
// event already corrected, and leaving the record reads permanently red.
func TestAcknowledgeSyncableDeadLetter(t *testing.T) {
	d, s := newWalDB(t)
	const id = "orders-sync"
	seedSyncableConfig(t, d, id)

	// No record yet — acknowledging nothing is a loud 404-class error.
	err := d.AcknowledgeSyncableDeadLetter(testCtx(t), id, 77)
	require.True(t, errors.Is(err, cluster.ErrNotDeadLettered), "got: %v", err)

	// A dead letter lands (proposed through Raft, as the worker would).
	dl := &cluster.SyncableDeadLetter{ID: id, Index: 77, TimestampUnixNano: 100, Kind: "permanent", Message: "NUL byte"}
	ent, err := cluster.NewUpsertSyncableDeadLetterEntity(dl)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{ent}}))

	count, acked, last, err := d.SyncableDeadLetterStats(id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count, "unacknowledged: reads red")
	require.Zero(t, acked)
	require.Equal(t, uint64(77), last)

	// Acknowledge — replicated through Raft; the completeness count goes
	// green, the audit trail stays.
	require.NoError(t, d.AcknowledgeSyncableDeadLetter(testCtx(t), id, 77))
	count, acked, _, err = d.SyncableDeadLetterStats(id)
	require.NoError(t, err)
	require.Zero(t, count, "acknowledged: the completeness check goes green")
	require.Equal(t, uint64(1), acked)

	recs, err := d.SyncableDeadLetters(id, 0, 10)
	require.NoError(t, err)
	require.Len(t, recs, 1, "the record stays listable — audit trail, not erasure")
	require.True(t, recs[0].Acknowledged)
	require.NotZero(t, recs[0].AcknowledgedAtUnixNano)

	// The skip itself stands: the worker's exclusion is bookkeeping-independent.
	has, err := s.HasSyncableDeadLetter(id, 77)
	require.NoError(t, err)
	require.True(t, has)

	// Idempotent: a second acknowledge is a clean no-op.
	require.NoError(t, d.AcknowledgeSyncableDeadLetter(testCtx(t), id, 77))
}
