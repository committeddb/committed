//go:build adversarial

package db_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/version"
)

// flapSource is a source database shared by every node, as a real one is:
// an append-only list of one-row transactions.
type flapSource struct {
	n atomic.Int64
}

// flapIngestable reads flapSource from the position it is given, one
// proposal per transaction, each bundling the position after it.
type flapIngestable struct {
	src *flapSource
	typ *cluster.Type
}

func (x *flapIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	next := 0
	if pos != nil {
		n, err := strconv.Atoi(strings.TrimPrefix(string(pos), "p-"))
		if err != nil {
			return err
		}
		next = n
	}
	for {
		if next >= int(x.src.n.Load()) {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		p := &cluster.Proposal{
			Entities:       []*cluster.Entity{{Type: x.typ, Key: fmt.Appendf(nil, "t%d", next), Data: []byte("v")}},
			SourceSeq:      uint64(next + 1),
			SourceTxnID:    fmt.Sprintf("tx-%d", next),
			TxnScopedDedup: true,
			Position:       cluster.Position(fmt.Sprintf("p-%d", next+1)),
		}
		select {
		case pr <- p:
			next++
		case <-ctx.Done():
			return nil
		}
	}
}

func (x *flapIngestable) Close() error { return nil }

func (x *flapIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

type flapParser struct{ ing *flapIngestable }

func (p *flapParser) Parse(*cluster.ParsedConfig) (cluster.Ingestable, error) { return p.ing, nil }

// flapLogKeys reads the topic's keys from one node's log, in log order.
func flapLogKeys(t *testing.T, s *wal.Storage, typeID string) []string {
	t.Helper()
	r := s.Reader("flap-verify")
	var keys []string
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			return keys
		}
		require.NoError(t, err)
		for _, e := range a.Entities {
			if e.ID == typeID {
				keys = append(keys, string(e.Key))
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Scenario: pipelined ingest under leader flaps.
//
// Invariant protected: an ingest worker keeps many source transactions in
// flight (feature level 3), and whatever leadership does, every node's log
// holds each source transaction exactly once, in source order. A transaction
// lost before commit must never be skipped by a later one that committed (the
// apply-side ordering check rejects the later one), and a deposed session's
// stragglers must never land behind its successor's re-emission.
//
// Setup: a source appended to continuously while the leader is partitioned
// away from the other two, repeatedly; then heal, stop appending, and compare
// every node's log with the source.
// -----------------------------------------------------------------------------
func TestAdversarial_PipelinedIngestUnderLeaderFlaps(t *testing.T) {
	h, fc := newFaultyMultiDBHarness(t, 3, 50*time.Millisecond, time.Second,
		db.WithVersionAnnounce(), db.WithIngestSupervisorInitialBackoff(20*time.Millisecond))
	defer h.Close()

	src := &flapSource{}
	ing := &flapIngestable{src: src}
	for _, n := range h.nodes {
		n.db.AddIngestableParser("flap", &flapParser{ing: ing})
	}

	h.WaitForLeader(t)
	leaderID := h.stableLeader()
	leader := h.dbByID(leaderID)
	require.Eventually(t, func() bool { return leader.FeatureEnabled(version.FeatureLevel) },
		20*time.Second, 20*time.Millisecond, "feature level never announced")

	const typeID = "flap-evt"
	seedUserProposals(t, leader, h.nodeByID(leaderID).storage, typeID, nil)
	typ, err := h.nodeByID(leaderID).storage.ResolveType(cluster.LatestTypeRef(typeID))
	require.NoError(t, err)
	ing.typ = typ

	const id = "flap-ingest"
	proposeRetryingLost(t, func() error {
		return leader.ProposeIngestable(testCtx(t), &cluster.Configuration{
			ID: id, MimeType: "text/toml", Data: []byte("[ingestable]\nname = \"" + id + "\"\ntype = \"flap\"\n"),
		})
	})

	// The source: one transaction every 3ms until told to stop.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Millisecond):
				src.n.Add(1)
			}
		}
	})

	all := []uint64{1, 2, 3}
	for flap := range 6 {
		time.Sleep(time.Second)
		var cur uint64
		require.Eventually(t, func() bool { cur = h.agreedLeaderAmong(all); return cur != 0 },
			30*time.Second, 20*time.Millisecond, "no agreed leader before flap %d", flap)
		var rest []uint64
		for _, id := range all {
			if id != cur {
				rest = append(rest, id)
			}
		}
		t.Logf("flap %d: partitioning leader %d at %d source transactions", flap, cur, src.n.Load())
		fc.Partition([]uint64{cur}, rest)
		elected := assert.Eventually(t, func() bool {
			l := h.agreedLeaderAmong(rest)
			return l != 0 && l != cur
		}, 30*time.Second, 20*time.Millisecond, "the majority never elected a new leader (flap %d)", flap)
		if !elected {
			if path := os.Getenv("FLAP_GOROUTINE_DUMP"); path != "" {
				buf := make([]byte, 64<<20)
				_ = os.WriteFile(path, buf[:runtime.Stack(buf, true)], 0o600)
			}
			t.FailNow()
		}
		time.Sleep(700 * time.Millisecond)
		fc.Heal()
	}
	close(stop)
	wg.Wait()

	n := int(src.n.Load())
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("t%d", i)
	}
	for _, node := range h.nodes {
		var got []string
		require.Eventually(t, func() bool {
			got = flapLogKeys(t, node.storage, typeID)
			return len(got) >= n
		}, 120*time.Second, 100*time.Millisecond, "node %d never caught up: %d of %d", node.id, len(got), n)
		if !slicesEqual(got, want) {
			t.Fatalf("node %d: the log does not hold the source exactly once in order: %s", node.id, firstDivergence(got, want))
		}
		seq, _ := node.storage.IngestOrder(id)
		require.Positive(t, seq, "node %d: nothing was sequenced; the scenario never pipelined", node.id)
		t.Logf("node %d: %d proposals rejected at apply", node.id, node.storage.IngestRejectedTotal())
	}
	t.Logf("%d source transactions, every node's log exact", n)
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firstDivergence describes where got departs from want.
func firstDivergence(got, want []string) string {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			lo, hi := max(i-3, 0), min(i+4, len(got))
			return fmt.Sprintf("at position %d (of %d logged, %d in the source): want %q, log has %v", i, len(got), len(want), want[i], got[lo:hi])
		}
	}
	return fmt.Sprintf("lengths differ: %d logged, %d in the source", len(got), len(want))
}
