package wal_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestApplyNeverBlocksOnWedgedListener pins the apply-liveness invariant from
// the field wedge: a runaway analyst query locked a sink table, the config
// listener stalled on an undeadlined ALTER, the bounded notification channel
// filled, and the raft apply loop blocked ON THE SEND — appliedIndex froze
// one entry behind commitIndex and every proposal cluster-wide timed out
// while /ready stayed green. The invariant: appliedIndex NEVER stalls on
// destination-DB (listener) state. Here the listener is maximally wedged — a
// 1-slot channel nobody reads — and applying far more config entries than
// any buffer must still complete promptly (the notify pump absorbs them).
func TestApplyNeverBlocksOnWedgedListener(t *testing.T) {
	dir := t.TempDir()
	syncCh := make(chan *db.SyncableWithID, 1) // wedged listener: never read

	// The configs must BUILD (a degraded config never reaches the notify
	// step), so register a fake syncable parser like the determinism suite.
	p := parser.New()
	fakeSyncParser := &clusterfakes.FakeSyncableParser{}
	fakeSyncParser.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("sql", fakeSyncParser)

	s, err := wal.Open(dir, p, syncCh, nil, wal.WithoutFsync())
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	const configs = 64 // 2x the old channel buffer — the pre-fix deadlock zone
	done := make(chan error, 1)
	go func() {
		for i := 0; i < configs; i++ {
			ent, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
				ID:       fmt.Sprintf("wedge-%d", i),
				MimeType: "application/json",
				Data:     []byte(fmt.Sprintf(`{"syncable":{"name":"wedge-%d","type":"sql"}}`, i)),
			})
			if err != nil {
				done <- err
				return
			}
			bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{ent}}).Marshal()
			if err != nil {
				done <- err
				return
			}
			term, index := uint64(1), uint64(i+1) //nolint:gosec // G115: small test loop counter
			pe := &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
			if err := s.Save(&defaultHardState, []*pb.Entry{pe}, &defaultSnap); err != nil {
				done <- err
				return
			}
			if err := s.ApplyCommitted(pe); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("apply blocked behind a wedged listener — the appliedIndex-never-stalls-on-destination-state invariant is broken (the cluster-wide write-freeze field incident)")
	}
	require.Equal(t, uint64(configs), s.AppliedIndex(),
		"every config entry applied despite the wedged listener")
	// Prove the notifications actually flowed AND the deferred builds work
	// (an unbuildable config would degrade to nil and make this test
	// vacuous): the wedged channel holds the pump's first delivery.
	select {
	case n := <-syncCh:
		require.NotNil(t, n.Build, "the queued notification defers the build to the listener")
		require.NotNil(t, n.Build(), "the queued build must produce the syncable — otherwise the configs never built and the test proved nothing")
	case <-time.After(5 * time.Second):
		t.Fatal("no notification ever reached the channel — the configs never queued and the test proved nothing")
	}
}
