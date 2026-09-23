package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPeerRecordReceiveBackends(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			payload := func(id uint64) []byte { return append(experimentEntry(t, id, pb.EntryNormal), 0xa0, 0x06, 0x07) }
			ten, twenty, thirty := payload(10), payload(20), payload(30)
			release := s.BeginCatchUp()
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(ten), frame(twenty))))
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(ten), frame(twenty), frame(thirty))))
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(thirty))))
			require.Equal(t, uint64(30), s.EventIndex())
			require.Equal(t, uint64(10), s.firstEventIndex.Load())
			require.EqualValues(t, 2, s.eventLogWriteOps.Load())
			require.Zero(t, s.AppliedIndex())
			// Validate the entire incoming batch before writing even its valid prefix.
			valid := fetchedStream(frame(payload(40)))
			corrupt := frame(payload(50))
			corrupt[len(corrupt)-1] ^= 1
			for _, bad := range [][]byte{
				append(append([]byte(nil), valid...), fetchedStream(corrupt)...),
				append(append([]byte(nil), valid...), 0x80),
				append(append([]byte(nil), valid...), fetchedStream(frame([]byte{0xff}))...),
			} {
				require.Error(t, s.AppendFetchedRecords(bad))
				require.Equal(t, uint64(30), s.EventIndex())
				require.EqualValues(t, 2, s.eventLogWriteOps.Load())
			}
			release()
			require.NoError(t, s.Close())
			restored, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = restored.Close() }()
			require.Equal(t, uint64(30), restored.EventIndex())
			cursor := restored.eventLog.records()
			defer func() { _ = cursor.Close() }()
			for i, want := range [][]byte{ten, twenty, thirty} {
				got, err := cursor.Seek(uint64(i+1) * 10)
				require.NoError(t, err)
				require.Equal(t, want, got.Payload)
			}
			require.NoError(t, restored.appendEvent(compressionTestEntry(40)))
			require.Equal(t, uint64(40), restored.EventIndex())
		})
	}
}

func TestPeerAndLocalAppendSerializeBackends(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			s, err := openStorage(t.TempDir(), nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			entries := make([]*pb.Entry, 0, 2)
			frames := make([][]byte, 0, 2)
			for _, id := range []uint64{10, 30} {
				raw := experimentEntry(t, id, pb.EntryNormal)
				entry := new(pb.Entry)
				require.NoError(t, proto.Unmarshal(raw, entry))
				entries = append(entries, entry)
				frames = append(frames, frame(raw))
			}
			data := fetchedStream(frames...)
			start := make(chan struct{})
			results := make(chan error, 2)
			go func() { <-start; results <- s.AppendFetchedRecords(data) }()
			go func() { <-start; results <- s.appendEvents(entries) }()
			close(start)
			first, second := <-results, <-results
			require.NoError(t, first)
			require.NoError(t, second)
			require.Equal(t, uint64(30), s.EventIndex())
			require.EqualValues(t, 1, s.eventLogWriteOps.Load())
		})
	}
}
