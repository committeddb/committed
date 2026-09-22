package wal

import (
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func TestLegacyDataHeadFallback(t *testing.T) {
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
			opts := *native.DefaultOptions
			opts.NoSync = true
			log, err := native.Open(t.TempDir(), &opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			if err := log.Write(1, frame(experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value")))); err != nil {
				t.Fatal(err)
			}
			for i := range tc.internalCount {
				raw := experimentEntry(t, uint64(i+20), pb.EntryConfChange)
				if tc.corrupt && i == tc.internalCount-1 {
					raw = []byte{0xff}
				}
				if err := log.Write(uint64(i+2), frame(raw)); err != nil {
					t.Fatal(err)
				}
			}
			s := &Storage{eventLog: tidwallbackend.OwnLegacy(log), logger: zap.NewNop()}
			s.dataEventIndex.Store(tc.persisted)
			s.recoverLegacyDataHead()
			if s.DataEventIndex() != tc.want {
				t.Fatal(s.DataEventIndex(), tc.want)
			}
		})
	}
}
