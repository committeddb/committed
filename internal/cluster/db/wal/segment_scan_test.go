package wal

import (
	"bytes"
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestSegmentScanRawRangeAndIdentity(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	records := [][]byte{experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 20, pb.EntryConfChange), experimentEntry(t, 100, pb.EntryNormal, experimentRow("key", "value"))}
	if err := adapter.appendRaw(records); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	err := adapter.scanRaw(t.Context(), segmentlog.Coverage{Start: 20, End: 101}, func(_ uint64, raw []byte) error { got = append(got, raw); return nil })
	if err != nil || len(got) != 2 || !bytes.Equal(got[0], records[1]) || !bytes.Equal(got[1], records[2]) {
		t.Fatal(got, err)
	}
	if err := adapter.log.Append([]segmentlog.Record{{ID: 200, Payload: experimentEntry(t, 201, pb.EntryNormal)}}); err != nil {
		t.Fatal(err)
	}
	called := false
	err = adapter.scanRaw(t.Context(), segmentlog.Coverage{Start: 200, End: 201}, func(uint64, []byte) error { called = true; return nil })
	if !errors.Is(err, ErrCorruptEntry) || called {
		t.Fatal("delivered mismatched entry", err)
	}
}
