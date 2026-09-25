package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestFetchedLogicalEntriesBackends(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			payload := func(id uint64) []byte {
				// Unknown protobuf fields must survive the receive path byte-for-byte.
				return append(experimentEntry(t, id, pb.EntryNormal), 0xa0, 0x06, 0x07)
			}
			ten, twenty, thirty := payload(10), payload(20), payload(30)
			observer := &productionAppendObserver{Appender: s.eventLog.entries}
			s.eventLog.entries = &observedEntryStore{entryStore: s.eventLog.entries, appender: observer}
			require.ErrorIs(t, s.appendFetchedEntries(0, [][]byte{ten}), eventlog.ErrInvalid, "catch-up fence required")
			release := s.BeginCatchUp()
			defer release()
			require.ErrorIs(t, s.appendFetchedEntries(7, [][]byte{ten}), eventlog.ErrInvalid, "do not mix generations")
			require.Zero(t, observer.calls)
			require.NoError(t, s.appendFetchedEntries(0, [][]byte{ten, twenty}))
			require.Equal(t, 1, observer.calls)
			require.Equal(t, uint64(20), s.EventIndex())
			require.Equal(t, uint64(10), s.firstEventIndex.Load())
			require.Zero(t, s.AppliedIndex(), "fetch does not apply application metadata")
			require.NoError(t, s.appendFetchedEntries(0, [][]byte{ten, twenty, thirty}))
			require.Equal(t, 2, observer.calls)
			require.Equal(t, []eventlog.Record{{ID: 10, Payload: ten}, {ID: 20, Payload: twenty}, {ID: 30, Payload: thirty}}, observer.records)
			require.NoError(t, s.appendFetchedEntries(0, [][]byte{ten, twenty, thirty}))
			require.NoError(t, s.appendFetchedEntries(0, nil))
			require.Equal(t, 2, observer.calls)
			for _, batch := range [][][]byte{
				{payload(40), {0xff}},
				{payload(40), payload(40)},
				{payload(50), payload(40)},
				{payload(0)},
				{payload(^uint64(0))},
				{{0xff}, payload(40)},
			} {
				require.ErrorIs(t, s.appendFetchedEntries(0, batch), eventlog.ErrInvalid)
				require.Equal(t, uint64(30), s.EventIndex())
				require.Equal(t, 2, observer.calls, "validate the entire batch before appending")
			}
			release()
			require.NoError(t, s.Close())
			reopened, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			require.Equal(t, uint64(30), reopened.EventIndex())
			require.Zero(t, reopened.AppliedIndex())
			// Read through the raw storage capability to ensure no re-marshalling
			// silently changed unknown fields on disk.
			for i, raw := range [][]byte{ten, twenty, thirty} {
				var stored []byte
				if reopened.eventLog.managed != nil {
					record, err := reopened.eventLog.managed.Read(uint64(i+1) * 10)
					require.NoError(t, err)
					stored = record.Payload
				} else {
					stored, err = reopened.readEventAt(uint64(i + 1))
					require.NoError(t, err)
				}
				require.Equal(t, raw, stored)
			}
		})
	}
}

func TestFetchedLogicalEntriesUseSelectedGeneration(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			s, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			ten := experimentEntry(t, 10, pb.EntryNormal)
			twenty := experimentEntry(t, 20, pb.EntryNormal)
			release := s.BeginCatchUp()
			require.NoError(t, s.appendFetchedEntries(0, [][]byte{ten}))
			release()
			_, err = s.eventLog.managed.Rewrite(t.Context(), 7, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
			require.NoError(t, err)
			require.Zero(t, s.EventLogGeneration(), "application completion has not caught up with publication")
			release = s.BeginCatchUp()
			defer release()
			require.ErrorIs(t, s.appendFetchedEntries(0, [][]byte{ten, twenty}), eventlog.ErrInvalid)
			require.NoError(t, s.appendFetchedEntries(7, [][]byte{ten, twenty}))
			_, err = s.eventLog.managed.Read(10)
			require.ErrorIs(t, err, eventlog.ErrNotFound, "overlap must not resurrect an erased record")
			record, err := s.eventLog.managed.Read(20)
			require.NoError(t, err)
			require.Equal(t, twenty, record.Payload)
			require.Equal(t, uint64(20), s.EventIndex())
		})
	}
}
