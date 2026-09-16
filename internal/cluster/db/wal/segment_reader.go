package wal

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// segmentActualReader is experimental and implements the streaming ActualReader
// contract. Its cursor is the last examined Raft index, never a physical offset.
// Each Read holds the adapter's read lock through decode and filtering, so it
// cannot combine records from different rewrite generations. No view is pinned
// between ordinary calls. protectedReaderAt adds a bounded multi-call lifetime;
// the caller owns adapter lifetime.
type segmentActualReader struct {
	mu       sync.Mutex
	events   *segmentEventLog
	resolver cluster.TypeResolver
	applied  func() uint64
	index    uint64
	pos      atomic.Uint64
	ctx      context.Context // optional lifetime for a protected reader
}

var _ db.ActualReader = (*segmentActualReader)(nil)

// readerAt resumes strictly after index. applied must return a monotonically
// advancing, concurrency-safe applied watermark; resolver must safely resolve
// the types visible through that watermark. Neither may reenter this adapter.
func (l *segmentEventLog) readerAt(index uint64, resolver cluster.TypeResolver, applied func() uint64) (*segmentActualReader, error) {
	if resolver == nil || applied == nil {
		return nil, segmentlog.ErrInvalid
	}
	return &segmentActualReader{events: l, index: index, resolver: resolver, applied: applied}, nil
}

func (r *segmentActualReader) Position() uint64 { return r.pos.Load() }

func (r *segmentActualReader) Read() (*cluster.Actual, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events.mu.RLock()
	defer r.events.mu.RUnlock()
	for {
		if r.ctx != nil {
			if err := context.Cause(r.ctx); err != nil {
				return nil, err
			}
		}
		if r.index == ^uint64(0) {
			return nil, io.EOF
		}
		index, raw, err := r.events.seekRawLocked(r.index + 1)
		if errors.Is(err, segmentlog.ErrNotFound) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		// Do not resolve types, advance the cursor, or report scan progress for
		// an entry that is durable but whose application has not finished.
		if index > r.applied() {
			return nil, io.EOF
		}
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			return nil, err
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
		if len(entities) > 0 {
			return &cluster.Actual{Index: index, Entities: entities}, nil
		}
	}
}
