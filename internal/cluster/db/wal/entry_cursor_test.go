package wal

import (
	"errors"
	"io"
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func TestEntryCursorRetainsLegacyDecode(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	for i, id := range []uint64{10, 30, 90} {
		if err := log.Write(uint64(i+1), frame(experimentEntry(t, id, pb.EntryNormal))); err != nil {
			t.Fatal(err)
		}
	}
	decoded := map[uint64]*pb.Entry{}
	calls := 0
	raw := tidwall.NewLegacyCursor(log, func(raw []byte) (uint64, *pb.Entry, error) {
		calls++
		payload, err := unframe(raw)
		if err != nil {
			return 0, nil, err
		}
		entry := new(pb.Entry)
		if err := proto.Unmarshal(payload, entry); err != nil {
			return 0, nil, err
		}
		decoded[entry.GetIndex()] = entry
		return entry.GetIndex(), entry, nil
	})
	var c entryCursor = &decodedEntryCursor{seek: raw.Seek, close: raw.Close}
	defer func() { _ = c.Close() }()
	if err := c.SeekGE(11); err != nil {
		t.Fatal(err)
	}
	current, err := c.Current()
	if err != nil || current.GetIndex() != 30 || current != decoded[30] {
		t.Fatal("lost selected decoded entry", current, err)
	}
	before := calls
	for range 3 {
		retry, err := c.Current()
		if err != nil || retry != current || calls != before {
			t.Fatal("retry repeated read/decode", err, calls)
		}
	}
	c.Advance()
	next, err := c.Current()
	if err != nil || next.GetIndex() != 90 || calls != before+1 || next != decoded[90] {
		t.Fatal("sequential entry decoded more than once", next, calls, err)
	}
	c.Advance()
	if _, err := c.Current(); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	if err := log.Write(4, frame(experimentEntry(t, 100, pb.EntryNormal))); err != nil {
		t.Fatal(err)
	}
	if next, err := c.Current(); err != nil || next.GetIndex() != 100 {
		t.Fatal(next, err)
	}
	if err := c.SeekGE(10); err != nil {
		t.Fatal(err)
	}
	if first, err := c.Current(); err != nil || first.GetIndex() != 10 {
		t.Fatal(first, err)
	}
}

func TestEntryCursorSharedBackends(t *testing.T) {
	for name, open := range eventLogTestBackends() {
		t.Run(name, func(t *testing.T) {
			log, err := open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			for _, id := range []uint64{10, 30, 90} {
				if err := log.Append([]eventlog.Record{{ID: id, Payload: experimentEntry(t, id, pb.EntryNormal)}}); err != nil {
					t.Fatal(err)
				}
			}
			c := newEventEntryCursor(log.NewCursor(), 0)
			defer func() { _ = c.Close() }()
			if err := c.SeekGE(11); err != nil {
				t.Fatal(err)
			}
			first, err := c.Current()
			if err != nil || first.GetIndex() != 30 {
				t.Fatal(first, err)
			}
			if retry, err := c.Current(); err != nil || retry != first {
				t.Fatal("decoded current entry again", err)
			}
			_, err = log.Rewrite(t.Context(), 2, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, r.ID != 30, nil })
			if err != nil {
				t.Fatal(err)
			}
			c.invalidate()
			if next, err := c.Current(); err != nil || next.GetIndex() != 90 {
				t.Fatal("lost seek target after rewrite", next, err)
			}
		})
	}
}

func TestEntryCursorPublicationInvalidatesUnconsumedEntry(t *testing.T) {
	for name, open := range eventLogTestBackends() {
		t.Run(name, func(t *testing.T) {
			log, err := open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			adapter := &eventLogAdapter{log: log}
			if err := adapter.appendRaw([][]byte{
				experimentEntry(t, 10, pb.EntryNormal, experimentRow("first", "data")),
				experimentEntry(t, 30, pb.EntryNormal, experimentRow("second", "data")),
			}); err != nil {
				t.Fatal(err)
			}
			var applied uint64
			reader, err := adapter.readerAt(0, eventTestResolver(eventTestType), func() uint64 { return applied })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			if _, err := reader.Read(); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if reader.cursor.current == nil || reader.cursor.current.GetIndex() != 10 {
				t.Fatal("unapplied entry was not retained")
			}
			_, err = adapter.rewriteRaw(t.Context(), 2, func(raw []byte) (bool, []byte, error) {
				entry := new(pb.Entry)
				if err := proto.Unmarshal(raw, entry); err != nil {
					return false, nil, err
				}
				return entry.GetIndex() != 10, raw, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			applied = 30
			if actual, err := reader.Read(); err != nil || actual.Index != 30 {
				t.Fatal("returned pre-publication retained entry", actual, err)
			}
		})
	}
}
