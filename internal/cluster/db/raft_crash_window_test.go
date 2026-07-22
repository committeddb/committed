package db_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// fatalSignalHook captures an async logger.Fatal (e.g. checkStorageInvariant in
// the serveChannels goroutine) by signaling a channel and then exiting only the
// calling goroutine — the rest of the process (Close cleanup) keeps running.
type fatalSignalHook struct{ ch chan<- string }

func (h *fatalSignalHook) OnWrite(ce *zapcore.CheckedEntry, _ []zapcore.Field) {
	select {
	case h.ch <- ce.Message:
	default:
	}
	runtime.Goexit()
}

// TestRaftRestart_CrashWindowGapWithLargeTail is the crash-loop regression: a
// node that crashed in the apply window (event log fsync'd through index N, the
// applied index persisted only up to `baseline`) must RECOVER on restart, not
// fatal-loop.
//
// With a >1 MiB uncompacted tail, raft re-delivers committed entries in 1 MiB
// (MaxCommittedSizePerReady) batches, so no single Ready drains the whole
// [baseline+1 .. N] tail. On today's code checkStorageInvariant fatals the
// moment it sees eventIndex(N) > appliedIndex (mid-replay) — permanently, on
// every boot. The fix: re-deliver only the unapplied tail (Config.Applied) and
// treat event-log-ahead-of-applied as the benign replay state it is.
func TestRaftRestart_CrashWindowGapWithLargeTail(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()

	// Phase 1 — WAL DB + a schemaless type; capture the applied index once the
	// type is committed but before any bulk data (the data entries must re-apply
	// against this already-applied type on restart).
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil, db.WithTickInterval(testTickInterval))
	require.Eventually(t, func() bool { return d.Leader() == 1 }, 5*time.Second, 5*time.Millisecond)

	proposeTypeTOML(t, d, "blob", "blob", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("blob"))
	require.NoError(t, err)
	var baseline uint64
	require.Eventually(t, func() bool {
		baseline = s.AppliedIndex()
		return baseline > 0 && baseline == s.EventIndex()
	}, 5*time.Second, 5*time.Millisecond)

	// Five ~600 KiB data proposals → a ~3 MiB tail above `baseline`, far over the
	// 1 MiB MaxCommittedSizePerReady so no single Ready can drain it.
	big := strings.Repeat("x", 600*1024)
	for i := 0; i < 5; i++ {
		pr := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: typ, Key: fmt.Appendf(nil, "k%d", i), Data: []byte(big),
		}}}
		require.NoError(t, d.Propose(testCtx(t), pr))
	}
	var N uint64
	require.Eventually(t, func() bool {
		N = s.AppliedIndex()
		return N == s.EventIndex() && N > baseline
	}, 5*time.Second, 5*time.Millisecond)

	sz, err := s.RaftLogApproxSize()
	require.NoError(t, err)
	require.Greater(t, sz, uint64(1<<20), "need a >1 MiB tail so the restart replay is batched")

	// Phase 2 — simulate the crash window: persist appliedIndex = baseline (type
	// applied, none of the ~3 MiB of data entries applied), leaving the event log
	// at N. This is the eventIndex-ahead-of-appliedIndex gap a crash between
	// appendEvent and saveAppliedIndex produces (widened by buffered bbolt writes
	// under WithoutFsync).
	require.NoError(t, d.Close())
	require.NoError(t, s.SetAppliedIndexForTest(baseline))
	require.NoError(t, s.Close())

	// Phase 3 — restart with a fatal-capturing logger; confirm the gap loaded.
	s2, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	require.Equal(t, baseline, s2.AppliedIndex())
	require.Equal(t, N, s2.EventIndex(), "event log is ahead of applied — the crash-window gap")

	fatalCh := make(chan string, 1)
	logger := zap.New(zapcore.NewNopCore(), zap.WithFatalHook(&fatalSignalHook{ch: fatalCh}))
	d2 := db.New(1, db.Peers{1: ""}, s2, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithLogger(logger))
	t.Cleanup(func() { _ = d2.Close(); _ = s2.Close() })

	// Today: the first partial-replay Ready leaves eventIndex > appliedIndex and
	// checkStorageInvariant fatals → crash loop. After the fix, replay drains the
	// tail without fatal-ing and appliedIndex recovers to N.
	select {
	case msg := <-fatalCh:
		t.Fatalf("crash-window restart fatal-looped (the bug): %q", msg)
	case <-time.After(2 * time.Second):
	}
	require.Eventually(t, func() bool { return s2.AppliedIndex() >= N }, 3*time.Second, 5*time.Millisecond,
		"restart replay must close the applied/event gap and advance appliedIndex to >= N")
}

// TestRaftRestart_LearnerMembershipSurvivesRestart drives a membership change
// through the REAL Ready loop and confirms the out-of-band durable state it
// produces survives a restart — the raft-loop half of the durability-watermark
// rule (docs/event-log-architecture.md). AddLearner runs the conf pre-pass
// (applyConfChange stages the ConfState and persists the peer URL) before
// ApplyCommittedBatch writes the ConfState atomically with the applied index,
// so both are durable once the applied index passes the conf entry. A learner
// (not a voter) is used so the change commits on a single node without needing
// the phantom peer's vote to leave the joint config.
func TestRaftRestart_LearnerMembershipSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()

	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil, db.WithTickInterval(testTickInterval))
	require.Eventually(t, func() bool { return d.Leader() == 1 }, 5*time.Second, 5*time.Millisecond)

	const peerURL = "http://127.0.0.1:29321"
	require.NoError(t, d.AddLearner(testCtx(t), 2, peerURL))
	require.Eventually(t, func() bool { _, ok := s.MemberPeerURLs()[2]; return ok },
		5*time.Second, 5*time.Millisecond, "learner-add must commit + apply")

	require.NoError(t, d.Close())
	require.NoError(t, s.Close())

	s2, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	defer s2.Close()
	_, cs, err := s2.InitialState()
	require.NoError(t, err)
	require.Contains(t, cs.GetLearners(), uint64(2), "learner membership must survive restart")
	require.Contains(t, cs.GetVoters(), uint64(1))
	require.Contains(t, s2.MemberPeerURLs(), uint64(2), "peer URL must survive restart")
}
