package wal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterpb"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestEventLogEventsScrubMatchesLegacyBytes(t *testing.T) {
	adapter, path := newSegmentedEventAdapter(t)
	deleted := &clusterpb.LogEntity{Type: &clusterpb.TypeRef{ID: "items", Version: 1}, Body: &clusterpb.LogEntity_Delete{Delete: &clusterpb.LogDelete{Key: []byte("gone")}}}
	inputs := [][]byte{
		experimentEntry(t, 1, pb.EntryNormal),
		append(experimentEntry(t, 2, pb.EntryConfChange), 0xa0, 0x06, 0x07), // unknown field survives verbatim
		experimentEntry(t, 10, pb.EntryNormal, experimentRow("gone", "private")),
		experimentEntry(t, 20, pb.EntryNormal, experimentRow("gone", "private"), experimentRow("safe", "keep")),
		experimentEntry(t, 30, pb.EntryNormal, experimentRow("snapshot", "old")),
		experimentEntry(t, 80, pb.EntryNormal, experimentRow("snapshot", "latest")),
		experimentEntry(t, 90, pb.EntryNormal, deleted),
	}
	legacy, err := tidwal.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	batch := new(tidwal.Batch)
	for i, raw := range inputs {
		batch.Write(uint64(i+1), frame(raw))
	}
	if err := legacy.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := adapter.appendRaw(inputs); err != nil {
		t.Fatal(err)
	}
	for i, raw := range inputs {
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			t.Fatal(err)
		}
		framed, err := legacy.Read(uint64(i + 1))
		if err != nil {
			t.Fatal(err)
		}
		old, err := unframe(framed)
		if err != nil {
			t.Fatal(err)
		}
		got, err := adapter.readRaw(entry.GetIndex())
		if err != nil || !bytes.Equal(old, got) {
			t.Fatal("raw-entry mismatch", err)
		}
	}
	sel := map[string]uint64{string(tombstoneKey("items", []byte("gone"))): 90}
	msel := map[string]uint64{string(tombstoneKey("items", []byte("snapshot"))): 80}
	filter := func(raw []byte) (bool, []byte, error) { return scrubFilterEntry(raw, sel, msel, 90) }
	expected := map[uint64][]byte{}
	for i := range inputs {
		framed, err := legacy.Read(uint64(i + 1))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := unframe(framed)
		if err != nil {
			t.Fatal(err)
		}
		keep, out, err := filter(raw)
		if err != nil {
			t.Fatal(err)
		}
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			t.Fatal(err)
		}
		if keep {
			expected[entry.GetIndex()] = bytes.Clone(out)
		}
	}
	result, err := adapter.rewriteRaw(t.Context(), 1, filter)
	if err != nil || !result.Published || result.ChangedRecords == 0 {
		t.Fatal(result, err)
	}
	if _, err := adapter.log.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := adapter.log.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := segmented.Open(path, segmentlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	adapter = &eventLogAdapter{log: reopened}
	if len(expected) != 5 {
		t.Fatal("unexpected scrub survivors", len(expected))
	}
	for _, raw := range inputs {
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			t.Fatal(err)
		}
		got, err := adapter.readRaw(entry.GetIndex())
		if want, ok := expected[entry.GetIndex()]; ok {
			if err != nil || !bytes.Equal(want, got) {
				t.Fatal("scrub mismatch", entry.GetIndex(), err)
			}
		} else if !errors.Is(err, eventlog.ErrNotFound) {
			t.Fatal(err)
		}
	}
	id, _, err := adapter.seekRaw(10)
	if err != nil || id != 20 {
		t.Fatal("erased checkpoint resume", id, err)
	}
	if _, _, err := adapter.seekRaw(91); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	// Assert domain semantics independently of the byte comparison.
	entry := new(pb.Entry)
	if err := proto.Unmarshal(expected[20], entry); err != nil {
		t.Fatal(err)
	}
	proposal := new(clusterpb.LogProposal)
	if err := proto.Unmarshal(entry.Data, proposal); err != nil {
		t.Fatal(err)
	}
	if len(proposal.LogEntities) != 1 || string(proposal.LogEntities[0].GetRow().GetKey()) != "safe" || proposal.RequestID != 123 {
		t.Fatal(proposal)
	}
	if err := proto.Unmarshal(expected[90], entry); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(entry.Data, proposal); err != nil {
		t.Fatal(err)
	}
	if !cluster.IsErasedKey(proposal.LogEntities[0].GetDelete().GetKey()) {
		t.Fatal("delete key not erased")
	}
	result, err = adapter.rewriteRaw(t.Context(), 2, filter)
	if err != nil || result.ChangedRecords != 0 {
		t.Fatal("non-idempotent scrub", result, err)
	}
}

func TestEventLogEventsRejectBadAppendBatch(t *testing.T) {
	for _, bad := range [][]byte{{0xff}, experimentEntry(t, 0, pb.EntryNormal), experimentEntry(t, ^uint64(0), pb.EntryNormal)} {
		adapter, _ := newSegmentedEventAdapter(t)
		err := adapter.appendRaw([][]byte{experimentEntry(t, 1, pb.EntryNormal), bad})
		if !errors.Is(err, eventlog.ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := adapter.readRaw(1); !errors.Is(err, eventlog.ErrNotFound) {
			t.Fatal("partially appended invalid batch", err)
		}
	}
}

func TestEventLogEventsRejectStoredIdentityMismatch(t *testing.T) {
	for _, payload := range [][]byte{{0xff}, experimentEntry(t, 2, pb.EntryNormal)} {
		adapter, _ := newSegmentedEventAdapter(t)
		if err := adapter.log.Append([]eventlog.Record{{ID: 1, Payload: payload}}); err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.readRaw(1); !errors.Is(err, ErrCorruptEntry) {
			t.Fatal(err)
		}
		if _, _, err := adapter.seekRaw(1); !errors.Is(err, ErrCorruptEntry) {
			t.Fatal(err)
		}
		called := false
		_, err := adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) { called = true; return false, nil, nil })
		if !errors.Is(err, ErrCorruptEntry) || called {
			t.Fatal("erased corrupt entry", err)
		}
	}
}

func TestEventLogEventsRejectReplacementIdentityChange(t *testing.T) {
	adapter, path := newSegmentedEventAdapter(t)
	original := experimentEntry(t, 10, pb.EntryNormal)
	if err := adapter.appendRaw([][]byte{original}); err != nil {
		t.Fatal(err)
	}
	_, err := adapter.rewriteRaw(t.Context(), 1, func([]byte) (bool, []byte, error) { return true, experimentEntry(t, 11, pb.EntryNormal), nil })
	if !errors.Is(err, ErrCorruptEntry) {
		t.Fatal(err)
	}
	if err := adapter.log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := segmented.Open(path, segmentlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	got, err := (&eventLogAdapter{log: log}).readRaw(10)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatal("published changed identity", err)
	}
}
