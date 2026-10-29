package wal

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestDataHeadFallback(t *testing.T) {
	for _, tc := range []struct {
		name            string
		internalCount   int
		corrupt         bool
		persisted, want uint64
	}{
		{"find-user-entry", 3, false, 0, 10},
		{"cap", 4096, false, 0, 0},
		{"corrupt", 3, true, 0, 0},
		{"persisted-wins", 4096, true, 50, 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := []eventlog.Record{{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))}}
			for i := range tc.internalCount {
				raw := experimentEntry(t, uint64(i+20), pb.EntryConfChange)
				if tc.corrupt && i == tc.internalCount-1 {
					raw = []byte{0xff}
				}
				records = append(records, eventlog.Record{ID: uint64(i + 20), Payload: raw})
			}
			s := &Storage{eventLog: productionBindingOf(t, records...), logger: zap.NewNop()}
			s.dataEventIndex.Store(tc.persisted)
			s.recoverDataHead()
			if s.DataEventIndex() != tc.want {
				t.Fatal(s.DataEventIndex(), tc.want)
			}
		})
	}
}
