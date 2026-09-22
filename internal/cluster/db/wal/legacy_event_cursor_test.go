package wal

import (
	"errors"
	"io"
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func TestProductionCursorRebindsAndCloses(t *testing.T) {
	open := func(ids ...uint64) *tidwallbackend.LegacyLog {
		t.Helper()
		log, err := native.Open(t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		for i, id := range ids {
			if err := log.Write(uint64(i+1), frame(experimentEntry(t, id, pb.EntryNormal, experimentRow("key", "value")))); err != nil {
				t.Fatal(err)
			}
		}
		return tidwallbackend.OwnLegacy(log)
	}
	s := &Storage{eventLog: open(10, 30)}
	ref := cluster.TypeRef{ID: "items", Version: 1}
	typ, _ := eventTestType(ref)
	s.typeCache.Store(ref, typeCacheEntry{t: typ})
	s.appliedIndex.Store(100)
	r := &Reader{s: s}
	if actual, err := r.Read(); err != nil || actual.Index != 10 {
		t.Fatal(actual, err)
	}
	// A handle change alone must invalidate the cursor, even without a
	// generation increment. The survivor has a different dense sequence.
	replacement := open(30)
	s.eventMu.Lock()
	err := s.eventLog.Close()
	s.eventLog = replacement
	s.eventMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if actual, err := r.Read(); err != nil || actual.Index != 30 {
		t.Fatal(actual, err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Read(); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
	if r.Position() != 30 {
		t.Fatal(r.Position())
	}
	empty := &Reader{s: s}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := empty.Read(); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
	exhausted := &Reader{s: s, raftIndex: ^uint64(0)}
	defer func() { _ = exhausted.Close() }()
	if _, err := exhausted.Read(); !errors.Is(err, io.EOF) {
		t.Fatal("checkpoint overflow restarted stream", err)
	}
}
