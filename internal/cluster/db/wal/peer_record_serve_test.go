package wal

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type recordServeSink struct {
	begin   func(uint64, uint64) error
	records func([]byte) error
	ended   bool
	result  db.EventServeResult
}

func (s *recordServeSink) Begin(generation, frontier uint64) error {
	if s.begin != nil {
		return s.begin(generation, frontier)
	}
	return nil
}

func (s *recordServeSink) Segment(string, int64, io.Reader) error {
	return errors.New("unexpected physical segment")
}

func (s *recordServeSink) Records(data []byte) error { return s.records(data) }

func (s *recordServeSink) End(result db.EventServeResult) error {
	s.ended, s.result = true, result
	return nil
}

func TestSharedServeExistingPeerRecords(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			source, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = source.Close() }()
			payloads := make([][]byte, 0, 3)
			for _, id := range []uint64{10, 20, 30} {
				payloads = append(payloads, append(experimentEntry(t, id, pb.EntryNormal), 0xa0, 0x06, 0x07))
			}
			require.NoError(t, source.AppendFetchedRecords(fetchedStream(frame(payloads[0]), frame(payloads[1]), frame(payloads[2]))))
			// Send the actual public serving path into every backend's receiver.
			for targetName, opener := range storageTestOpeners() {
				t.Run(targetName, func(t *testing.T) {
					target, err := openStorage(t.TempDir(), nil, nil, nil, opener, WithSafeMode())
					require.NoError(t, err)
					defer func() { _ = target.Close() }()
					sink := &recordServeSink{records: target.AppendFetchedRecords}
					result, err := source.ServeEvents(t.Context(), 10, 25, sink)
					require.NoError(t, err)
					require.True(t, sink.ended)
					require.Equal(t, result, sink.result)
					require.Equal(t, uint64(20), result.LastIndex)
					require.Equal(t, uint64(30), result.EventIndex)
					require.False(t, result.More)
					cursor := target.eventLog.records()
					defer func() { _ = cursor.Close() }()
					record, err := cursor.Seek(1)
					require.NoError(t, err)
					require.Equal(t, payloads[1], record.Payload)
					_, err = cursor.Seek(21)
					require.ErrorIs(t, err, eventlog.ErrNotFound)
				})
			}
			// The byte budget applies to encoded bytes; an oversized record still
			// makes progress. A second call resumes at the last delivered ID.
			var stream []byte
			sink := &recordServeSink{records: func(data []byte) error { stream = data; return nil }}
			release := source.FreezeEventLayout()
			result, err := source.serveRecordEvents(t.Context(), 0, 30, sink, 1)
			release()
			require.NoError(t, err)
			require.Equal(t, fetchedStream(frame(payloads[0])), stream)
			require.Equal(t, uint64(10), result.LastIndex)
			require.True(t, result.More)
			release = source.FreezeEventLayout()
			result, err = source.serveRecordEvents(t.Context(), 20, 30, sink, 1)
			release()
			require.NoError(t, err)
			require.Equal(t, fetchedStream(frame(payloads[2])), stream)
			require.False(t, result.More)
			// A concurrent append in Begin is outside the captured frontier.
			sink = &recordServeSink{
				begin: func(_, frontier uint64) error {
					require.Equal(t, uint64(30), frontier)
					return source.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 40, pb.EntryNormal))))
				},
				records: func(data []byte) error { stream = data; return nil },
			}
			result, err = source.ServeEvents(t.Context(), 20, 100, sink)
			require.NoError(t, err)
			require.Equal(t, uint64(30), result.LastIndex)
			require.Equal(t, fetchedStream(frame(payloads[2])), stream)
		})
	}
}

func TestSharedServeErrorsAndErasedSuffix(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented"} {
		t.Run(name, func(t *testing.T) {
			source, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = source.Close() }()
			require.NoError(t, source.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)), frame(experimentEntry(t, 20, pb.EntryNormal)))))
			_, err = source.eventLog.managed.Rewrite(t.Context(), 7, func(record eventlog.Record) ([]byte, bool, error) { return record.Payload, record.ID == 10, nil })
			require.NoError(t, err)
			sink := &recordServeSink{records: func([]byte) error { return nil }}
			result, err := source.ServeEvents(t.Context(), 0, 20, sink)
			require.NoError(t, err)
			require.Equal(t, uint64(7), result.Generation)
			require.Equal(t, uint64(20), result.EventIndex)
			require.Equal(t, uint64(10), result.LastIndex, "erased progress is not a delivered record")
			require.False(t, result.More)
			result, err = source.ServeEvents(t.Context(), 10, 20, sink)
			require.NoError(t, err)
			require.Zero(t, result.LastIndex)
			require.False(t, result.More)
			errSink := errors.New("sink refused")
			for _, atBegin := range []bool{true, false} {
				sink = &recordServeSink{records: func([]byte) error { return errSink }}
				if atBegin {
					sink.begin = func(uint64, uint64) error { return errSink }
				}
				_, err = source.ServeEvents(t.Context(), 0, 20, sink)
				require.ErrorIs(t, err, errSink)
				require.False(t, sink.ended)
			}
			ctx, cancel := context.WithCancel(t.Context())
			sink = &recordServeSink{begin: func(uint64, uint64) error { cancel(); return nil }, records: func([]byte) error { t.Fatal("sent records after cancellation"); return nil }}
			_, err = source.ServeEvents(ctx, 0, 20, sink)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, sink.ended)
			release, allowed := source.eventLayout.move()
			require.True(t, allowed, "failed serving must release its freeze")
			release()
		})
	}
}
