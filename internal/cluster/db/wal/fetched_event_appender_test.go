package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func fetchedStream(raws ...[]byte) []byte {
	var data []byte
	for _, raw := range raws {
		data = binary.AppendUvarint(data, uint64(len(raw)))
		data = append(data, raw...)
	}
	return data
}

func TestFetchedAppenderPreservesNativeBytesAndLocalProgress(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	s := &Storage{eventLog: tidwallbackend.OwnLegacy(log)}
	if err := s.appendEvent(&pb.Entry{Index: proto.Uint64(10), Type: pb.EntryNormal.Enum()}); err != nil {
		t.Fatal(err)
	}
	unframed := experimentEntry(t, 20, pb.EntryNormal)
	if err := s.AppendFetchedRecords(fetchedStream(unframed)); !errors.Is(err, ErrCorruptEntry) {
		t.Fatal("accepted unframed peer bytes", err)
	}
	// Preserve unknown protobuf fields along with the exact checksum envelope.
	legacy := frame(append(unframed, 0xa0, 0x06, 0x01))
	checksummed := frame(experimentEntry(t, 30, pb.EntryNormal))
	if err := s.AppendFetchedRecords(fetchedStream(legacy, checksummed)); err != nil {
		t.Fatal(err)
	}
	for i, want := range [][]byte{legacy, checksummed} {
		got, err := log.Read(uint64(i + 2))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("fetch changed peer encoding", err)
		}
	}
	if err := s.AppendFetchedRecords(fetchedStream(legacy, checksummed)); err != nil {
		t.Fatal(err)
	}
	if err := s.appendEvent(&pb.Entry{Index: proto.Uint64(40), Type: pb.EntryNormal.Enum()}); err != nil {
		t.Fatal("local writer retained stale fetched frontier", err)
	}
	if s.EventIndex() != 40 || s.firstEventIndex.Load() != 10 || s.eventLogWriteOps.Load() != 3 {
		t.Fatal("progress or replay accounting changed")
	}
	if last, err := log.LastIndex(); err != nil || last != 4 {
		t.Fatal(last, err)
	}
}

func TestFetchedAndLocalAppendSerialize(t *testing.T) {
	for range 12 {
		opts := *native.DefaultOptions
		opts.NoSync = true
		log, err := native.Open(t.TempDir(), &opts)
		if err != nil {
			t.Fatal(err)
		}
		s := &Storage{eventLog: tidwallbackend.OwnLegacy(log)}
		entries := []*pb.Entry{
			{Index: proto.Uint64(10), Type: pb.EntryNormal.Enum()},
			{Index: proto.Uint64(30), Type: pb.EntryNormal.Enum()},
		}
		data := fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)), frame(experimentEntry(t, 30, pb.EntryNormal)))
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Go(func() { <-start; errs <- s.appendEvents(entries) })
		wg.Go(func() { <-start; errs <- s.AppendFetchedRecords(data) })
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
		if last, err := log.LastIndex(); err != nil || last != 2 || s.EventIndex() != 30 || s.eventLogWriteOps.Load() != 1 {
			t.Error("concurrent replay wrote twice", last, err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
