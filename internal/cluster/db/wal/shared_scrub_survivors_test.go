package wal

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterpb"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type observedDeleteCandidates struct {
	entryStore
	seeks   []uint64
	cursors int
}

func (s *observedDeleteCandidates) NewEntryCursor(index uint64) entryCursor {
	s.cursors++
	return &observedDeleteCursor{entryCursor: s.entryStore.NewEntryCursor(index), owner: s}
}

type observedDeleteCursor struct {
	entryCursor
	owner *observedDeleteCandidates
}

func (c *observedDeleteCursor) SeekGE(index uint64) error {
	c.owner.seeks = append(c.owner.seeks, index)
	return c.entryCursor.SeekGE(index)
}

func TestSharedScrubDeleteCandidates(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, corrupt := range []bool{false, true} {
			label := name + "/survivors"
			if corrupt {
				label = name + "/corrupt-candidate"
			}
			t.Run(label, func(t *testing.T) {
				s, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				deleted := func(key []byte) *clusterpb.LogEntity {
					return &clusterpb.LogEntity{Type: &clusterpb.TypeRef{ID: "items", Version: 1}, Body: &clusterpb.LogEntity_Delete{Delete: &clusterpb.LogDelete{Key: key}}}
				}
				erased := experimentEntry(t, 20, pb.EntryNormal, deleted(cluster.ErasedKey))
				if corrupt {
					erased = []byte{0xff}
				}
				require.NoError(t, s.eventLog.entries.Append([]eventlog.Record{
					// A completion rescan would decode this unrelated record and fail.
					{ID: 1, Payload: []byte{0xff}},
					{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, deleted(cluster.ErasedKey), deleted([]byte("retained")), deleted([]byte("also-retained")))},
					{ID: 20, Payload: erased},
					{ID: 40, Payload: experimentEntry(t, 40, pb.EntryNormal, experimentRow("sibling", "survived"))},
					{ID: 50, Payload: experimentEntry(t, 50, pb.EntryNormal, deleted([]byte("later")))},
				}))
				observed := &observedDeleteCandidates{entryStore: s.eventLog.entries}
				s.eventLog.entries = observed
				raws, err := s.sharedScrubDeleteSurvivors(t.Context(), 80)
				require.NoError(t, err)
				require.Empty(t, raws)
				require.Zero(t, observed.cursors, "an empty candidate index needs no event reads")
				for _, index := range []uint64{10, 20, 30, 40, 50, 70, 100} {
					require.NoError(t, s.recordUnhashedDelete(index))
				}
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				_, err = s.sharedScrubDeleteSurvivors(ctx, 80)
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, observed.cursors)
				raws, err = s.sharedScrubDeleteSurvivors(t.Context(), 80)
				if corrupt {
					require.Error(t, err)
					require.Nil(t, raws, "discard partial survivors on a decoding failure")
					require.Equal(t, 7, s.PendingDeleteKeyErasures())
					return
				}
				require.NoError(t, err)
				require.Equal(t, []rawDelete{{index: 10}, {index: 50}}, raws,
					"mixed entries retain one cadence row; erased and missing deletes do not")
				require.Equal(t, []uint64{10, 20, 30, 40, 50, 70}, observed.seeks)
				require.Equal(t, 1, observed.cursors)
				require.NoError(t, s.reconcileUnhashedDeletes(80, 0, raws, nil))
				var indexes []uint64
				require.NoError(t, s.view(func(tx *bolt.Tx) error {
					return tx.Bucket(unhashedDeleteBucket).ForEach(func(k, _ []byte) error {
						indexes = append(indexes, binary.BigEndian.Uint64(k))
						return nil
					})
				}))
				require.Equal(t, []uint64{10, 50, 100}, indexes, "leave post-bound work untouched")
			})
		}
	}
}
