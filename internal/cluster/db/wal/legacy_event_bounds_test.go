package wal

import (
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"

	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func TestLegacyEventBoundsPreserveProgressOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name                string
		entries             [][]byte
		wantFirst, wantLast uint64
		wantError           bool
	}{
		{"empty", nil, 0, 0, false},
		{"sparse", [][]byte{experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 90, pb.EntryNormal)}, 10, 90, false},
		{"corrupt-head", [][]byte{{0xff}, experimentEntry(t, 90, pb.EntryNormal)}, 7, 100, true},
		{"corrupt-tail", [][]byte{experimentEntry(t, 10, pb.EntryNormal), {0xff}}, 7, 100, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, err := native.Open(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			for i, raw := range tc.entries {
				if err := log.Write(uint64(i+1), frame(raw)); err != nil {
					t.Fatal(err)
				}
			}
			s := &Storage{eventLog: bindLegacyEventLog(tidwallbackend.OwnLegacy(log), nil)}
			s.firstEventIndex.Store(7)
			s.eventIndex.Store(100)
			err = s.deriveEventBoundsLocked()
			if (err != nil) != tc.wantError || s.firstEventIndex.Load() != tc.wantFirst || s.EventIndex() != tc.wantLast {
				t.Fatal(s.firstEventIndex.Load(), s.EventIndex(), err)
			}
		})
	}
}

func TestLegacyScrubBoundsRejectChangedTailBeforePublishing(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	s := &Storage{eventLog: bindLegacyEventLog(tidwallbackend.OwnLegacy(log), nil)}
	s.eventIndex.Store(100)
	s.firstEventIndex.Store(7)
	checkRejected := func() {
		t.Helper()
		if err := s.recomputeEventBoundsLocked(); err == nil {
			t.Fatal("accepted missing or changed tail")
		}
		if s.EventIndex() != 100 || s.firstEventIndex.Load() != 7 {
			t.Fatal("rejected bounds changed progress")
		}
	}
	checkRejected()
	if err := log.Write(1, frame(experimentEntry(t, 90, pb.EntryNormal))); err != nil {
		t.Fatal(err)
	}
	checkRejected()
	if err := log.Write(2, frame(experimentEntry(t, 100, pb.EntryNormal))); err != nil {
		t.Fatal(err)
	}
	if err := s.recomputeEventBoundsLocked(); err != nil {
		t.Fatal(err)
	}
	if s.firstEventIndex.Load() != 90 || s.EventIndex() != 100 {
		t.Fatal("did not publish valid bounds")
	}
}
