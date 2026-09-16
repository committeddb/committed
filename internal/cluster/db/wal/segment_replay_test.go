package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestSegmentReplayPreservesErasureAfterReopen(t *testing.T) {
	adapter, path := newSegmentEventExperiment(t)
	if index, err := adapter.eventIndex(); err != nil || index != 0 {
		t.Fatal(index, err)
	}
	originals := [][]byte{experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 40, pb.EntryNormal), experimentEntry(t, 100, pb.EntryNormal)}
	if index, err := adapter.appendCommittedRaw(originals); err != nil || index != 100 {
		t.Fatal(index, err)
	}
	if _, err := adapter.rewriteRaw(t.Context(), 1, func([]byte) (bool, []byte, error) { return false, nil, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.log.Reclaim(t.Context()); err != nil {
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
	adapter = &eventLogAdapter{log: log}
	if index, err := adapter.eventIndex(); err != nil || index != 100 {
		t.Fatal("lost erased frontier", index, err)
	}
	// A completely replayed batch must not create files or modify any bytes.
	files, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	contents := map[string][]byte{}
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(path, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		contents[f.Name()] = data
	}
	for _, batch := range [][][]byte{nil, originals} {
		if index, err := adapter.appendCommittedRaw(batch); err != nil || index != 100 {
			t.Fatal(index, err)
		}
	}
	after, err := os.ReadDir(path)
	if err != nil || len(after) != len(files) {
		t.Fatal(err)
	}
	for _, f := range after {
		data, err := os.ReadFile(filepath.Join(path, f.Name()))
		if err != nil || !bytes.Equal(data, contents[f.Name()]) {
			t.Fatal("replay changed disk", f.Name(), err)
		}
	}
	replay := make([][]byte, 0, len(originals)+1)
	replay = append(replay, originals...)
	replay = append(replay, experimentEntry(t, 150, pb.EntryNormal))
	if index, err := adapter.appendCommittedRaw(replay); err != nil || index != 150 {
		t.Fatal(index, err)
	}
	for _, id := range []uint64{10, 40, 100} {
		if _, err := adapter.readRaw(id); !errors.Is(err, eventlog.ErrNotFound) {
			t.Fatal("resurrected erased record", id, err)
		}
	}
	if _, err := adapter.readRaw(150); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentReplayValidatesSkippedPrefix(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	first := experimentEntry(t, 10, pb.EntryNormal)
	if _, err := adapter.appendCommittedRaw([][]byte{first}); err != nil {
		t.Fatal(err)
	}
	next := experimentEntry(t, 20, pb.EntryNormal)
	for _, batch := range [][][]byte{
		{first, first, next},
		{first, experimentEntry(t, 5, pb.EntryNormal), next},
		{{0xff}, next},
		{experimentEntry(t, 0, pb.EntryNormal), next},
		{next, first},
	} {
		if _, err := adapter.appendCommittedRaw(batch); !errors.Is(err, eventlog.ErrInvalid) {
			t.Fatal(err)
		}
		if index, err := adapter.eventIndex(); err != nil || index != 10 {
			t.Fatal("invalid batch advanced frontier", index, err)
		}
		if _, err := adapter.readRaw(20); !errors.Is(err, eventlog.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if err := adapter.log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.appendCommittedRaw([][]byte{first}); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal("replay bypassed closed handle", err)
	}
}

func TestSegmentReplaySerializesConcurrentBatches(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	batch := [][]byte{experimentEntry(t, 1, pb.EntryNormal), experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 100, pb.EntryNormal)}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			index, err := adapter.appendCommittedRaw(batch)
			if err == nil && index != 100 {
				err = errors.New("wrong frontier")
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{1, 10, 100} {
		if _, err := adapter.readRaw(id); err != nil {
			t.Fatal(err)
		}
	}
}
