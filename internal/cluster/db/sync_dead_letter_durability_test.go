package db_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// permanentErrorSyncable rejects every user entry with a permanent error —
// the auto-dead-letter path.
type permanentErrorSyncable struct{}

func (s *permanentErrorSyncable) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(a) {
		return false, nil
	}
	return false, cluster.Permanent(fmt.Errorf("row will never apply"))
}
func (s *permanentErrorSyncable) Close() error { return nil }

type permanentErrorParser struct{}

func (p *permanentErrorParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return &permanentErrorSyncable{}, nil
}

// TestSyncDeadLetter_OrphanedRecordDoesNotAdvanceCheckpoint pins the durable-
// record-before-advance invariant: when the dead-letter propose is orphaned (a
// leader flap between worker iterations), the worker must HOLD POSITION and
// re-run the decide+record — the checkpoint must never durably pass a skipped
// index that has no dead-letter record (which would make the skip invisible to
// GET .../errors and unreplayable forever). Pre-fix, the failure path logged a
// Warn and advanced anyway; with the record's only propose attempt failing,
// the record never existed while the checkpoint moved past the skip.
func TestSyncDeadLetter_OrphanedRecordDoesNotAdvanceCheckpoint(t *testing.T) {
	dir := t.TempDir()
	const id = "dl-durable"

	p := parser.New()
	p.AddSyncableParser("test", &permanentErrorParser{})
	syncCh := make(chan *db.SyncableWithID)
	s, err := wal.Open(dir, p, syncCh, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close() })

	// Inject exactly one orphaned dead-letter propose — the leader-flap window.
	var fails atomic.Int32
	d.SetDeadLetterProposeHookForTest(func(*cluster.SyncableDeadLetter) error {
		if fails.Add(1) == 1 {
			return db.ErrProposalUnknown
		}
		return nil
	})

	configureDeleteSyncable(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"poison"})

	// The record must EXIST despite the first propose being orphaned: the
	// worker held position and re-recorded on its next iteration.
	var poisonIndex uint64
	require.Eventually(t, func() bool {
		dls, derr := d.SyncableDeadLetters(id, 0, 10)
		if derr != nil || len(dls) == 0 {
			return false
		}
		poisonIndex = dls[0].Index
		return true
	}, 10*time.Second, 10*time.Millisecond,
		"the skip's dead-letter record must eventually be durable — an orphaned propose must be retried, not dropped")
	require.GreaterOrEqual(t, fails.Load(), int32(2), "the injected orphan must have forced a re-record")

	// And only then does the checkpoint pass the skipped index.
	require.Eventually(t, func() bool {
		cp, cerr := s.GetSyncableIndex(id)
		return cerr == nil && cp >= poisonIndex
	}, 10*time.Second, 10*time.Millisecond,
		"once the record is durable the worker advances normally")
}
