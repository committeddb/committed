package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// panicRecorder counts implementation panics so the test can observe that the
// worker survived one and RETRIED (two panics = one recover + one retry).
type panicRecorder struct {
	mu     sync.Mutex
	panics int
}

func (r *panicRecorder) bump() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.panics++
	return r.panics
}

func (r *panicRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.panics
}

type panicWorkerSyncable struct{ rec *panicRecorder }

func (s *panicWorkerSyncable) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(a) {
		return false, nil
	}
	s.rec.bump()
	panic("deterministic implementation bug on this entry")
}
func (s *panicWorkerSyncable) Close() error { return nil }

type panicWorkerParser struct{ rec *panicRecorder }

func (p *panicWorkerParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return &panicWorkerSyncable{rec: p.rec}, nil
}

// TestSyncWorker_PanicDoesNotCrashNode_EntersStuckFlow is the whole-path proof
// of the trust-boundary conversion: a syncable whose implementation
// deterministically PANICS on an entry must not unwind the process (pre-fix it
// did, and the restart resumed into the same entry — a node crash-loop). The
// panic converts to a transient error at the boundary, so the worker RETRIES
// (observably: repeated panics), and the existing stuck flow fires its
// replicated record — the operator's documented skip/replay path, with no new
// operational state.
func TestSyncWorker_PanicDoesNotCrashNode_EntersStuckFlow(t *testing.T) {
	dir := t.TempDir()
	const id = "panicking-sync"
	rec := &panicRecorder{}

	p := parser.New()
	p.AddSyncableParser("test", &panicWorkerParser{rec: rec})
	syncCh := make(chan *db.SyncableWithID)
	s, err := wal.Open(dir, p, syncCh, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, nil,
		db.WithTickInterval(testTickInterval),
		db.WithSyncStuckThreshold(100*time.Millisecond))
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"boom"})

	// One recover + at least one retry proves the panic crossed the boundary as
	// an error and the node (this test process) survived it.
	require.Eventually(t, func() bool { return rec.count() >= 2 },
		10*time.Second, 10*time.Millisecond,
		"the worker must survive the panic and retry the entry")

	// The transient-retry loop must feed the EXISTING stuck machinery: the
	// replicated stuck record appears once the debounce threshold passes.
	require.Eventually(t, func() bool {
		stuck, ok, err := d.SyncableStuck(id)
		return err == nil && ok && stuck.Index > 0
	}, 10*time.Second, 10*time.Millisecond,
		"a deterministic panic must surface through the documented stuck flow (alert → inspect → skip/replay)")
}
