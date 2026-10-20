package wal

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Backend construction belongs in test fixtures, not the application adapter.
func eventLogTestBackends() map[string]func(string) (eventlog.EventLog, error) {
	return map[string]func(string) (eventlog.EventLog, error){
		"segmented-cached": func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}, Cache: segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}})
		},
		"segmented": func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}})
		},
	}
}

// productionBinding binds a fresh segmented log holding raws (unframed
// raftpb.Entry bytes, in raft-index order) as production Storage binds its
// event log.
func productionBinding(t testing.TB, raws ...[]byte) *eventLogBinding {
	t.Helper()
	log, err := eventLogTestBackends()["segmented"](t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	for _, raw := range raws {
		if err := log.Append([]eventlog.Record{{ID: rawIndex(t, raw), Payload: raw}}); err != nil {
			t.Fatal(err)
		}
	}
	return bindEventLog(log)
}

// productionBindingOf is productionBinding over records given whole, for a
// payload that does not decode.
func productionBindingOf(t testing.TB, records ...eventlog.Record) *eventLogBinding {
	t.Helper()
	binding := productionBinding(t)
	for _, r := range records {
		if err := binding.managed.Append([]eventlog.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	return binding
}

// rawIndex is the raft index an unframed raftpb.Entry carries.
func rawIndex(t testing.TB, raw []byte) uint64 {
	t.Helper()
	var entry pb.Entry
	if err := proto.Unmarshal(raw, &entry); err != nil {
		t.Fatal(err)
	}
	return entry.GetIndex()
}
