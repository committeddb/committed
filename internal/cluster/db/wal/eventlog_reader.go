package wal

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// eventActualReader is experimental and implements the streaming ActualReader
// contract. Its cursor is the last examined Raft index, never a physical offset.
// Each Read holds the adapter's read lock through decode and filtering, so it
// cannot combine records from different rewrite generations. No view is pinned
// between ordinary calls. protectedReaderAt adds a bounded multi-call lifetime;
// the caller owns adapter lifetime.
type eventActualReader struct {
	mu       sync.Mutex
	events   *eventLogAdapter
	cursor   *decodedEntryCursor
	epoch    uint64
	closed   bool
	resolver cluster.TypeResolver
	applied  func() uint64
	index    uint64
	pos      atomic.Uint64
	ctx      context.Context // optional lifetime for a protected reader
}

var _ db.ActualReader = (*eventActualReader)(nil)

// readerAt resumes strictly after index. applied must return a monotonically
// advancing, concurrency-safe applied watermark; resolver must safely resolve
// the types visible through that watermark. Neither may reenter this adapter.
func (l *eventLogAdapter) readerAt(index uint64, resolver cluster.TypeResolver, applied func() uint64) (*eventActualReader, error) {
	if resolver == nil || applied == nil {
		return nil, eventlog.ErrInvalid
	}
	return &eventActualReader{events: l, index: index, resolver: resolver, applied: applied, cursor: newEventEntryCursor(l.log.NewCursor(), index+1)}, nil
}

func (r *eventActualReader) Position() uint64 { return r.pos.Load() }

func (r *eventActualReader) Read() (*cluster.Actual, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if r.ctx != nil && r.ctx.Err() != nil {
			return nil, context.Cause(r.ctx)
		}
		return nil, eventlog.ErrClosed
	}
	r.events.mu.RLock()
	defer r.events.mu.RUnlock()
	if r.epoch != r.events.readEpoch {
		r.cursor.invalidate()
		r.epoch = r.events.readEpoch
	}
	for {
		if r.ctx != nil {
			if err := context.Cause(r.ctx); err != nil {
				return nil, err
			}
		}
		if r.index == ^uint64(0) {
			return nil, io.EOF
		}
		entry, err := r.cursor.Current()
		if errors.Is(err, eventlog.ErrNotFound) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		index := entry.GetIndex()
		// Do not resolve types, advance the cursor, or report scan progress for
		// an entry that is durable but whose application has not finished.
		if index > r.applied() {
			return nil, io.EOF
		}
		var entities []*cluster.Entity
		if entry.GetType() == pb.EntryNormal && entry.Data != nil {
			proposal := new(cluster.Proposal)
			if err := proposal.Unmarshal(entry.Data, r.resolver); err != nil {
				var unknown *cluster.UnknownReservedTypeError
				if !errors.As(err, &unknown) || !unknown.Skippable() {
					return nil, err
				}
			} else {
				entities = userTopicEntities(proposal.Entities)
			}
		}
		if r.ctx != nil {
			if err := context.Cause(r.ctx); err != nil {
				return nil, err
			}
		}
		r.index = index
		r.pos.Store(index)
		r.cursor.Advance()
		if len(entities) > 0 {
			return &cluster.Actual{Index: index, Entities: entities}, nil
		}
	}
}

// Close releases this reader's retained segment contents. It does not close the
// underlying log or affect other readers.
func (r *eventActualReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.cursor.Close()
}
