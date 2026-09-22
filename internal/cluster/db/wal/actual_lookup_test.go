package wal

import (
	"errors"
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func TestActualLookupSharedBackends(t *testing.T) {
	for name, open := range eventLogTestBackends() {
		t.Run(name, func(t *testing.T) {
			log, err := open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			raw := experimentEntry(t, 30, pb.EntryNormal, experimentRow("key", "value"))
			if err := log.Append([]eventlog.Record{{ID: 30, Payload: raw}}); err != nil {
				t.Fatal(err)
			}
			cursor := newEventEntryCursor(log.NewCursor(), 0)
			defer func() { _ = cursor.Close() }()
			entry, err := exactEntry(cursor, 30)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := actualFromEntry(entry, eventTestResolver(eventTestType))
			if err != nil || actual.Index != 30 || len(actual.Entities) != 1 {
				t.Fatal(actual, err)
			}
			if _, err := exactEntry(cursor, 29); !errors.Is(err, ErrActualNotFound) {
				t.Fatal(err)
			}
		})
	}
}

func TestProductionActualLookupRebindsAfterSwap(t *testing.T) {
	open := func(ids ...uint64) *tidwallbackend.LegacyLog {
		t.Helper()
		log, err := native.Open(t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		for i, id := range ids {
			raw := experimentEntry(t, id, pb.EntryNormal, experimentRow("key", "value"))
			if err := log.Write(uint64(i+1), frame(raw)); err != nil {
				t.Fatal(err)
			}
		}
		return tidwallbackend.OwnLegacy(log)
	}
	s := &Storage{eventLog: open(10, 30)}
	ref := cluster.TypeRef{ID: "items", Version: 1}
	typ, _ := eventTestType(ref)
	s.typeCache.Store(ref, typeCacheEntry{t: typ})
	// Replay lookup preserves its existing behavior even before applied advances.
	if actual, err := s.ActualAt(30); err != nil || actual.Index != 30 {
		t.Fatal(actual, err)
	}
	replacement := open(30)
	s.eventMu.Lock()
	err := s.eventLog.Close()
	s.eventLog = replacement
	s.scrubGen.Add(1)
	s.eventMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ActualAt(10); !errors.Is(err, ErrActualNotFound) {
		t.Fatal("erased entry remained visible", err)
	}
	if actual, err := s.ActualAt(30); err != nil || actual.Index != 30 {
		t.Fatal("lookup retained old handle or physical sequence", actual, err)
	}
}
