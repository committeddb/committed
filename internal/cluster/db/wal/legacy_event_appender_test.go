package wal

import (
	"bytes"
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

type productionAppendObserver struct {
	eventlog.Appender
	calls   int
	records []eventlog.Record
}

func (o *productionAppendObserver) Append(records []eventlog.Record) error {
	o.calls++
	for _, r := range records {
		o.records = append(o.records, eventlog.Record{ID: r.ID, Payload: bytes.Clone(r.Payload)})
	}
	return o.Appender.Append(records)
}

func TestProductionEventAppendUsesLogicalBackend(t *testing.T) {
	path := t.TempDir()
	log, err := native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &Storage{eventLog: tidwallbackend.OwnLegacy(log)}
	t.Cleanup(func() { _ = s.eventLog.Close() })
	observer := &productionAppendObserver{Appender: s.eventAppenderLocked()}
	s.eventAppender = observer
	entries := []*pb.Entry{
		{Index: proto.Uint64(10), Term: proto.Uint64(2), Type: pb.EntryNormal.Enum(), Data: []byte("first")},
		{Index: proto.Uint64(20), Term: proto.Uint64(2), Type: pb.EntryNormal.Enum(), Data: []byte("second")},
	}
	if err := s.appendEvents(entries); err != nil {
		t.Fatal(err)
	}
	if observer.calls != 1 || len(observer.records) != 2 {
		t.Fatal("production append bypassed backend", observer.calls, observer.records)
	}
	for i, entry := range entries {
		raw, err := proto.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		if observer.records[i].ID != entry.GetIndex() || !bytes.Equal(observer.records[i].Payload, raw) {
			t.Fatal("backend received physical framing or sequence ID")
		}
		stored, err := log.Read(uint64(i + 1))
		if err != nil || !bytes.Equal(stored, frame(raw)) {
			t.Fatal("legacy disk bytes changed", err)
		}
	}
	if err := s.appendEvents(entries); err != nil {
		t.Fatal(err)
	}
	if observer.calls != 1 || s.EventIndex() != 20 || s.firstEventIndex.Load() != 10 || s.eventLogWriteOps.Load() != 1 {
		t.Fatal("replay or progress semantics changed")
	}
	// Scrub and peer fetch replace the native handle under eventMu. A later
	// production append must bind to the replacement instead of the closed handle.
	s.eventMu.Lock()
	err = log.Close()
	if err == nil {
		s.eventLog, err = tidwallbackend.OpenLegacy(path, tidwallbackend.LegacyOptions{})
	}
	s.eventMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	last := &pb.Entry{Index: proto.Uint64(30), Term: proto.Uint64(2), Type: pb.EntryNormal.Enum(), Data: []byte("third")}
	if err := s.appendEvent(last); err != nil {
		t.Fatal("append retained closed native handle", err)
	}
	if observer.calls != 1 {
		t.Fatal("old backend used after handle replacement")
	}
	stored, err := s.nativeEventTransferLocked().Read(3)
	raw, marshalErr := proto.Marshal(last)
	if err != nil || marshalErr != nil || !bytes.Equal(stored, frame(raw)) {
		t.Fatal("single-entry append changed legacy encoding", err, marshalErr)
	}
	if s.EventIndex() != 30 || s.firstEventIndex.Load() != 10 || s.eventLogWriteOps.Load() != 2 {
		t.Fatal("single-entry append changed progress")
	}
}
