package db_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// newWalDBOpts is newWalDB with db options — the oversize tests shrink the
// proposal size cap so a modest batch genuinely trips ErrProposalTooLarge.
func newWalDBOpts(t *testing.T, opts ...db.Option) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)

	id := uint64(1)
	peers := db.Peers{id: ""}
	allOpts := append([]db.Option{db.WithTickInterval(testTickInterval)}, opts...)
	d := db.New(id, peers, s, p, nil, nil, allOpts...)
	t.Cleanup(func() {
		_ = d.Close()
		_ = s.Close()
	})
	return d, s
}

// oversizeIngestable emits one data proposal, then (optionally) a standalone
// position checkpoint — the minimal shape of the silent-loss bug: a dropped
// batch followed by an out-of-band checkpoint that commits past it.
type oversizeIngestable struct {
	proposal *cluster.Proposal
	position cluster.Position // sent after the proposal when non-nil
	handed   chan struct{}    // closed after the proposal hand-off returns
}

func (o *oversizeIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	select {
	case pr <- o.proposal:
		close(o.handed)
	case <-ctx.Done():
		return nil
	}
	if o.position != nil {
		select {
		case po <- o.position:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func (o *oversizeIngestable) Close() error { return nil }

func (o *oversizeIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// countTopicRows scans the permanent event log and counts entities of typ.
func countTopicRows(t *testing.T, s *wal.Storage, typeID string) int {
	t.Helper()
	r := s.Reader("oversize-verify")
	n := 0
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		for _, e := range a.Entities {
			if e.ID == typeID {
				n++
			}
		}
	}
	return n
}

func oversizeTestTopic(t *testing.T, d *db.DB, s *wal.Storage) *cluster.Type {
	t.Helper()
	proposeTypeTOML(t, d, "fat", "fat", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("fat"))
	require.NoError(t, err)
	return typ
}

// TestIngestOversize_BatchFreezesAndHoldsPosition: an oversized multi-entity
// batch freezes the worker whole — a proposal is an opaque atomic unit; the
// worker never splits it to make it fit (composition belongs to the emitter,
// which byte-budgets via sql.ChunkEntitiesByBytes). Neither its rows nor its
// bundled checkpoint may partially commit, and a later standalone checkpoint
// must never advance past the hole.
func TestIngestOversize_BatchFreezesAndHoldsPosition(t *testing.T) {
	d, s := newWalDBOpts(t, db.WithMaxProposalBytes(4096))
	const id = "oversize-batch"
	seedIngestableConfig(t, d, id)
	typ := oversizeTestTopic(t, d, s)

	const rows = 8
	entities := make([]*cluster.Entity, rows)
	for i := range entities {
		entities[i] = &cluster.Entity{
			Type: typ,
			Key:  fmt.Appendf(nil, "k%d", i),
			Data: append(fmt.Appendf(nil, "v%d-", i), make([]byte, 1024)...),
		}
	}
	ing := &oversizeIngestable{
		proposal: &cluster.Proposal{Entities: entities, Position: cluster.Position("cp-batch")},
		position: cluster.Position("cp-after"),
		handed:   make(chan struct{}),
	}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	<-ing.handed
	require.Never(t, func() bool {
		return len(s.Position(id)) != 0
	}, 700*time.Millisecond, 20*time.Millisecond,
		"no checkpoint may commit after an oversized batch is rejected")
	require.Zero(t, countTopicRows(t, s, typ.ID), "no partial batch may land in the log")
}

// TestIngestOversize_SingleEntityFreezesAndHoldsPosition: a single row whose
// lone proposal exceeds the cap cannot split — the worker must freeze, and the
// later standalone checkpoint must NEVER commit (pre-fix it did: the drop was
// warn-and-continue and the checkpoint advanced past the lost row).
func TestIngestOversize_SingleEntityFreezesAndHoldsPosition(t *testing.T) {
	d, s := newWalDBOpts(t, db.WithMaxProposalBytes(1024))
	const id = "oversize-freeze"
	seedIngestableConfig(t, d, id)
	typ := oversizeTestTopic(t, d, s)

	ing := &oversizeIngestable{
		proposal: &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: typ,
			Key:  []byte("big"),
			Data: make([]byte, 4096),
		}}},
		position: cluster.Position("cp-after"),
		handed:   make(chan struct{}),
	}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	<-ing.handed
	// The position must never advance: not to cp-after, not to anything.
	require.Never(t, func() bool {
		return len(s.Position(id)) != 0
	}, 700*time.Millisecond, 20*time.Millisecond,
		"a checkpoint must not commit past a dropped row (silent CDC loss)")
	require.Zero(t, countTopicRows(t, s, typ.ID))
}

// TestIngestOversize_CDCSeqCannotSplitFreezes: an oversized CDC proposal
// (SourceSeq > 0) must not split — sub-proposals would share the seq and the
// dedup highwater would drop the second half. The worker freezes instead.
func TestIngestOversize_CDCSeqCannotSplitFreezes(t *testing.T) {
	d, s := newWalDBOpts(t, db.WithMaxProposalBytes(1024))
	const id = "oversize-cdc"
	seedIngestableConfig(t, d, id)
	typ := oversizeTestTopic(t, d, s)

	entities := make([]*cluster.Entity, 4)
	for i := range entities {
		entities[i] = &cluster.Entity{
			Type: typ,
			Key:  fmt.Appendf(nil, "k%d", i),
			Data: make([]byte, 512),
		}
	}
	ing := &oversizeIngestable{
		proposal: &cluster.Proposal{Entities: entities, SourceSeq: 7},
		position: cluster.Position("cp-after"),
		handed:   make(chan struct{}),
	}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	<-ing.handed
	require.Never(t, func() bool {
		return len(s.Position(id)) != 0
	}, 700*time.Millisecond, 20*time.Millisecond,
		"an unsplittable CDC proposal must freeze, not advance the checkpoint")
	require.Zero(t, countTopicRows(t, s, typ.ID))
}
