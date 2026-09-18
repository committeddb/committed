package wal

import (
	"context"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// metadataSupersessions computes snapshot selections from a coherent log prefix.
// It shares the legacy selection rules. The caller must supply an authorized,
// applied scrub bound and coordinate the selection with subsequent publication;
// this call alone does not retain protection across a later rewrite. RTBF
// tombstone selections and delete-key erasure gates remain application-owned.
func (l *eventLogAdapter) metadataSupersessions(ctx context.Context, bound uint64) (map[string]uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.metadataSupersessionsLocked(ctx, bound)
}

// metadataSupersessionsLocked requires the adapter read or write lock.
func (l *eventLogAdapter) metadataSupersessionsLocked(ctx context.Context, bound uint64) (map[string]uint64, error) {
	selection := newMetadataSelection()
	end := bound
	if end < ^uint64(0) {
		end++
	}
	err := l.scanRawLocked(ctx, eventlog.Coverage{Start: 1, End: end}, func(_ uint64, raw []byte) error {
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			return err
		}
		return selection.observe(entry)
	})
	if err != nil {
		return nil, err
	}
	return selection.latest, nil
}
