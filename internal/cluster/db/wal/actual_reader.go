package wal

import (
	"context"
	"errors"
	"io"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// readCursorActual is the shared application loop for production and experimental
// backends. The caller serializes its reader and holds publication protection
// through this call. Cursor entries are already decoded and remain read-only.
//
// Durable entries can precede their applied type metadata. Such entries return
// temporary EOF without advancing. Resolution errors likewise retain the current
// entry; only deliberate skips and successfully interpreted entries make progress.
// ctx is optional and supplies the protected-reader cancellation boundary.
func readCursorActual(cursor entryCursor, after uint64, resolver cluster.TypeResolver, applied func() uint64, progress func(uint64), ctx context.Context) (*cluster.Actual, error) {
	for {
		if ctx != nil {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
		}
		if after == ^uint64(0) {
			return nil, io.EOF
		}
		entry, err := cursor.Current()
		if errors.Is(err, eventlog.ErrNotFound) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		index := entry.GetIndex()
		// Do not resolve types, advance the cursor, or report scan progress for
		// an entry that is durable but whose application has not finished.
		if index > applied() {
			return nil, io.EOF
		}
		var entities []*cluster.Entity
		if entry.GetType() == pb.EntryNormal && entry.Data != nil {
			proposal := new(cluster.Proposal)
			if err := proposal.Unmarshal(entry.Data, resolver); err != nil {
				var unknown *cluster.UnknownReservedTypeError
				if !errors.As(err, &unknown) || !unknown.Skippable() {
					return nil, err
				}
			} else {
				entities = userTopicEntities(proposal.Entities)
			}
		}
		if ctx != nil {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
		}
		after = index
		progress(index)
		cursor.Advance()
		if len(entities) > 0 {
			return &cluster.Actual{Index: index, Entities: entities}, nil
		}
	}
}
