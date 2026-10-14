package db_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/version"
)

// newOrderedDB is a single-node DB whose feature level is announced, so its
// ingest workers number and pipeline their ordered proposals.
func newOrderedDB(t *testing.T, opts ...db.Option) (*db.DB, *wal.Storage) {
	t.Helper()
	p := parser.New()
	s, err := wal.Open(t.TempDir(), p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	all := append([]db.Option{db.WithTickInterval(testTickInterval), db.WithVersionAnnounce()}, opts...)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil, all...)
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	require.Eventually(t, func() bool { return d.FeatureEnabled(version.FeatureLevel) },
		10*time.Second, 10*time.Millisecond, "feature level never announced")
	return d, s
}

// orderedIngestable emits one CDC transaction per proposal, each bundling the
// position after it ("p-<n>"), and resumes from the position it is given.
// It records each OnDurable and how often it handed a transaction over
// before its predecessor was reported durable — never, unless the worker
// keeps several in flight.
type orderedIngestable struct {
	typ *cluster.Type
	n   int
	// pauseAt, on the first session only, waits on gate before handing
	// over transaction pauseAt (paused is closed when it gets there).
	pauseAt int
	gate    chan struct{}
	paused  chan struct{}

	mu            sync.Mutex
	sessions      int
	durable       []int
	isDurable     map[int]bool
	handedTooSoon int
}

func newOrderedIngestable(typ *cluster.Type, n int) *orderedIngestable {
	return &orderedIngestable{typ: typ, n: n, pauseAt: -1, isDurable: map[int]bool{}}
}

func (x *orderedIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	start := 0
	if pos != nil {
		n, err := strconv.Atoi(strings.TrimPrefix(string(pos), "p-"))
		if err != nil {
			return err
		}
		start = n
	}
	x.mu.Lock()
	x.sessions++
	first := x.sessions == 1
	x.mu.Unlock()
	for i := start; i < x.n; i++ {
		if first && i == x.pauseAt {
			close(x.paused)
			select {
			case <-x.gate:
			case <-ctx.Done():
				return nil
			}
		}
		i := i
		p := &cluster.Proposal{
			Entities:       []*cluster.Entity{{Type: x.typ, Key: fmt.Appendf(nil, "k-%d", i), Data: []byte("v")}},
			SourceSeq:      uint64(i + 1),
			SourceTxnID:    fmt.Sprintf("tx-%d", i),
			TxnScopedDedup: true,
			Position:       cluster.Position(fmt.Sprintf("p-%d", i+1)),
			OnDurable: func() {
				x.mu.Lock()
				defer x.mu.Unlock()
				x.durable = append(x.durable, i)
				x.isDurable[i] = true
			},
		}
		select {
		case pr <- p:
		case <-ctx.Done():
			return nil
		}
		x.mu.Lock()
		if i > start && !x.isDurable[i-1] {
			x.handedTooSoon++
		}
		x.mu.Unlock()
	}
	<-ctx.Done()
	return nil
}

func (x *orderedIngestable) Close() error { return nil }

func (x *orderedIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (x *orderedIngestable) snapshot() (sessions int, durable []int, tooSoon int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.sessions, append([]int(nil), x.durable...), x.handedTooSoon
}

// topicKeys reads a topic's keys from the log, in log order.
func topicKeys(t *testing.T, s *wal.Storage, typeID string) []string {
	t.Helper()
	r := s.Reader("order-verify")
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

func txnKeys(from, to int) []string {
	var keys []string
	for i := from; i < to; i++ {
		keys = append(keys, fmt.Sprintf("k-%d", i))
	}
	return keys
}

// TestIngestOrder_PipelinesTransactions: at the feature level, the worker
// hands a transaction to raft before the previous one is durable, and the
// log still holds every transaction once, in source order, each reported
// durable once and in order.
func TestIngestOrder_PipelinesTransactions(t *testing.T) {
	d, s := newOrderedDB(t)
	const id = "ordered"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	const n = 200
	ing := newOrderedIngestable(typ, n)
	require.NoError(t, d.Ingest(context.Background(), id, ing))
	require.Eventually(t, func() bool { return string(s.Position(id)) == fmt.Sprintf("p-%d", n) },
		20*time.Second, 5*time.Millisecond, "every transaction must commit")

	require.Equal(t, txnKeys(0, n), topicKeys(t, s, typ.ID))
	seq, _ := s.IngestOrder(id)
	require.Equal(t, uint64(n), seq)
	require.Eventually(t, func() bool { _, durable, _ := ing.snapshot(); return len(durable) == n },
		5*time.Second, 5*time.Millisecond)
	_, durable, tooSoon := ing.snapshot()
	for i, v := range durable {
		require.Equal(t, i, v, "OnDurable out of order")
	}
	require.Positive(t, tooSoon, "the worker never had two transactions in flight")
}

// TestIngestOrder_SerialBelowTheFeatureLevel: until every member can apply
// the ordering check, ordered proposals go one at a time, unnumbered.
func TestIngestOrder_SerialBelowTheFeatureLevel(t *testing.T) {
	d, s := newWalDB(t)
	const id = "serial"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	const n = 30
	ing := newOrderedIngestable(typ, n)
	require.NoError(t, d.Ingest(context.Background(), id, ing))
	require.Eventually(t, func() bool { return string(s.Position(id)) == fmt.Sprintf("p-%d", n) },
		20*time.Second, 5*time.Millisecond)

	require.Equal(t, txnKeys(0, n), topicKeys(t, s, typ.ID))
	seq, _ := s.IngestOrder(id)
	require.Zero(t, seq, "nothing is numbered below the feature level")
	_, _, tooSoon := ing.snapshot()
	require.Zero(t, tooSoon)
}

// TestIngestOrder_RejectionFreezesAndResumes: another session's proposal
// commits ahead of the worker's next one (a straggler, as after a leader
// change), so apply rejects the worker's. The worker freezes, the supervisor
// restarts it from the durable position, and the new session continues
// after the straggler: every transaction lands once, in order.
func TestIngestOrder_RejectionFreezesAndResumes(t *testing.T) {
	d, s := newOrderedDB(t, db.WithIngestSupervisorInitialBackoff(10*time.Millisecond))
	const id = "rejected"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	const n, pause = 20, 10
	ing := newOrderedIngestable(typ, n)
	ing.pauseAt, ing.gate, ing.paused = pause, make(chan struct{}), make(chan struct{})
	require.NoError(t, d.Ingest(context.Background(), id, ing))
	<-ing.paused
	require.Eventually(t, func() bool { return string(s.Position(id)) == fmt.Sprintf("p-%d", pause) },
		10*time.Second, 5*time.Millisecond)

	last, _ := s.IngestOrder(id)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{
		Entities:           []*cluster.Entity{{Type: typ, Key: []byte("straggler"), Data: []byte("v")}},
		IngestableID:       id,
		IngestSeq:          last + 1,
		IngestSession:      12345,
		IngestSessionStart: true,
	}))
	close(ing.gate)

	require.Eventually(t, func() bool { return string(s.Position(id)) == fmt.Sprintf("p-%d", n) },
		20*time.Second, 5*time.Millisecond, "the restarted session must finish")
	want := append(txnKeys(0, pause), "straggler")
	want = append(want, txnKeys(pause, n)...)
	require.Equal(t, want, topicKeys(t, s, typ.ID))
	sessions, _, _ := ing.snapshot()
	require.GreaterOrEqual(t, sessions, 2, "the rejection must restart the worker")
}

// reemitIngestable sends a two-part transaction, sends both parts again
// (as a dialect does after reconnecting from its last handed-off position),
// then a third transaction.
type reemitIngestable struct{ typ *cluster.Type }

func (x *reemitIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	part := func(key, txn string, seq uint64, pos string) *cluster.Proposal {
		p := &cluster.Proposal{
			Entities:       []*cluster.Entity{{Type: x.typ, Key: []byte(key), Data: []byte("v")}},
			SourceSeq:      seq,
			SourceTxnID:    txn,
			TxnScopedDedup: true,
		}
		if pos != "" {
			p.Position = cluster.Position(pos)
		}
		return p
	}
	for _, p := range []*cluster.Proposal{
		part("a1", "A", 1, ""), part("a2", "A", 2, "pA"),
		part("a1", "A", 1, ""), part("a2", "A", 2, "pA"),
		part("b1", "B", 3, "pB"),
	} {
		select {
		case pr <- p:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func (x *reemitIngestable) Close() error { return nil }

func (x *reemitIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// TestIngestOrder_DedupsAReemittedPartStillInFlight: a part re-emitted while
// its first copy may still be in flight is a duplicate of what the session
// submitted, though not yet of anything applied.
func TestIngestOrder_DedupsAReemittedPartStillInFlight(t *testing.T) {
	d, s := newOrderedDB(t)
	const id = "reemit"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	require.NoError(t, d.Ingest(context.Background(), id, &reemitIngestable{typ: typ}))
	require.Eventually(t, func() bool { return string(s.Position(id)) == "pB" },
		10*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"a1", "a2", "b1"}, topicKeys(t, s, typ.ID))
}
