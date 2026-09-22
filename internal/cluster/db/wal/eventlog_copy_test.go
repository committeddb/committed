package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	legacy "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type copyBackend struct {
	name   string
	create func(string) (eventlog.EventLog, error)
	open   func(string) (eventlog.EventLog, error)
}

func copyBackends() []copyBackend {
	result := make([]copyBackend, 0, 4)
	for _, compressed := range []bool{false, true} {
		encoding := segmentlog.NoCompression
		if compressed {
			encoding = segmentlog.ZstdDefault
		}
		result = append(result, copyBackend{
			name: fmt.Sprintf("tidwall/compressed=%t", compressed),
			create: func(path string) (eventlog.EventLog, error) {
				return tidwall.Create(path, 1, tidwall.Options{SegmentBytes: 32 << 10, Compress: compressed})
			},
			open: func(path string) (eventlog.EventLog, error) { return tidwall.Open(path) },
		}, copyBackend{
			name: fmt.Sprintf("segmented/compressed=%t", compressed),
			create: func(path string) (eventlog.EventLog, error) {
				return segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 32 << 10, Encoding: segmentlog.Options{Compression: encoding}})
			},
			open: func(path string) (eventlog.EventLog, error) { return segmented.Open(path, segmentlog.Options{}) },
		})
	}
	return result
}

func copyEntry(t *testing.T, id uint64, size int) []byte {
	t.Helper()
	raw, err := proto.Marshal(&pb.Entry{Index: proto.Uint64(id), Term: proto.Uint64(2), Data: bytes.Repeat([]byte("x"), size)})
	if err != nil {
		t.Fatal(err)
	}
	// Unknown protobuf field 100: preserve bytes rather than re-marshal entries.
	return append(raw, 0xa0, 0x06, 0x01)
}

func copySource(t *testing.T, frames [][]byte, head uint64, compressed bool) *Storage {
	t.Helper()
	options := &legacy.Options{NoCopy: true, NoSync: true, SegmentSize: 32 << 10}
	if compressed {
		options.SealedSegmentCompression = legacy.CompressionZstd
	}
	log, err := legacy.Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	for i, raw := range frames {
		if err := log.Write(uint64(i+1), raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	if compressed {
		drainChurnCompression(t, log)
	}
	source := &Storage{eventLog: tidwall.OwnLegacy(log)}
	source.eventIndex.Store(head)
	return source
}

type copyObserver struct {
	eventlog.EventLog
	batches     int
	afterAppend func(int) error
}

func (l *copyObserver) Append(records []eventlog.Record) error {
	if len(records) == 0 || len(records) > eventCopyBatchRecords {
		return errors.New("unbounded copy batch count")
	}
	size := 0
	for _, r := range records {
		size += len(r.Payload)
	}
	if len(records) > 1 && size > eventCopyBatchBytes {
		return errors.New("unbounded copy batch bytes")
	}
	if err := l.EventLog.Append(records); err != nil {
		return err
	}
	l.batches++
	if l.afterAppend != nil {
		return l.afterAppend(l.batches)
	}
	return nil
}

func TestEventLogCopyBackends(t *testing.T) {
	for _, backend := range copyBackends() {
		t.Run(backend.name, func(t *testing.T) {
			// Exercise count and byte limits, an oversized singleton, and NoCopy reads.
			raw := make([][]byte, 600)
			frames := make([][]byte, len(raw))
			for i := range raw {
				size := 100
				if i >= 300 {
					size = 9000
				}
				if i == 500 {
					size = eventCopyBatchBytes + 10
				}
				raw[i] = copyEntry(t, uint64((i+1)*10), size)
				frames[i] = frame(raw[i])
			}
			for _, compressed := range []bool{false, true} {
				t.Run(fmt.Sprintf("source-compressed=%t", compressed), func(t *testing.T) {
					source := copySource(t, frames, 6000, compressed)
					path := t.TempDir()
					destination, err := backend.create(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = destination.Close() })
					observed := &copyObserver{EventLog: destination}
					if err := source.copyEventLog(t.Context(), observed); err != nil {
						t.Fatal(err)
					}
					if observed.batches < 4 {
						t.Fatal("copy did not exercise batch limits", observed.batches)
					}
					if err := destination.Close(); err != nil {
						t.Fatal(err)
					}
					destination, err = backend.open(path)
					if err != nil {
						t.Fatal(err)
					}
					adapter := &eventLogAdapter{log: destination}
					if index, err := adapter.eventIndex(); err != nil || index != 6000 {
						t.Fatal(index, err)
					}
					for i, expected := range raw {
						got, err := adapter.readRaw(uint64((i + 1) * 10))
						if err != nil || !bytes.Equal(got, expected) {
							t.Fatal("copy differs", i, err)
						}
						got, err = source.readEventAt(uint64(i + 1))
						if err != nil || !bytes.Equal(got, expected) {
							t.Fatal("source changed", i, err)
						}
					}
					next := copyEntry(t, 6010, 10)
					if index, err := adapter.appendCommittedRaw(append(raw, next)); err != nil || index != 6010 {
						t.Fatal(index, err)
					}
					if err := source.copyEventLog(t.Context(), destination); !errors.Is(err, eventlog.ErrInvalid) {
						t.Fatal("accepted nonempty destination", err)
					}
				})
			}
		})
	}
}

func TestEventLogCopyRejectsInvalidHistory(t *testing.T) {
	cases := []struct {
		name   string
		frames [][]byte
		head   uint64
	}{
		{"checksum", [][]byte{[]byte("unframed")}, 10},
		{"protobuf", [][]byte{frame([]byte{0xff})}, 10},
		{"zero-index", [][]byte{frame(copyEntry(t, 0, 10))}, 10},
		{"duplicate", [][]byte{frame(copyEntry(t, 10, 10)), frame(copyEntry(t, 10, 10))}, 10},
		{"decreasing", [][]byte{frame(copyEntry(t, 20, 10)), frame(copyEntry(t, 10, 10))}, 20},
		{"head-ahead", [][]byte{frame(copyEntry(t, 10, 10))}, 20},
		{"head-behind", [][]byte{frame(copyEntry(t, 10, 10))}, 5},
		{"missing-history", nil, 10},
	}
	for _, backend := range copyBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					source := copySource(t, tc.frames, tc.head, false)
					destination, err := backend.create(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = destination.Close() }()
					if err := source.copyEventLog(t.Context(), destination); !errors.Is(err, ErrCorruptEntry) {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestEventLogCopyFailureAndEmpty(t *testing.T) {
	for _, backend := range copyBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, mode := range []string{"empty", "canceled-before", "canceled-after-batch", "append-failure", "late-corruption", "erased-destination"} {
				t.Run(mode, func(t *testing.T) {
					destination, err := backend.create(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = destination.Close() }()
					frames := make([][]byte, 300)
					for i := range frames {
						frames[i] = frame(copyEntry(t, uint64(i+1), 10))
					}
					head := uint64(300)
					if mode == "empty" {
						frames, head = nil, 0
					}
					if mode == "late-corruption" {
						frames[299] = []byte("broken frame")
					}
					source := copySource(t, frames, head, false)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					expected := error(nil)
					observed := &copyObserver{EventLog: destination}
					switch mode {
					case "canceled-before":
						cancel()
						expected = context.Canceled
					case "canceled-after-batch":
						observed.afterAppend = func(int) error { cancel(); return nil }
						expected = context.Canceled
					case "append-failure":
						expected = errors.New("injected append failure after sync")
						observed.afterAppend = func(int) error { return expected }
					case "late-corruption":
						expected = ErrCorruptEntry
					case "erased-destination":
						if err := destination.Append([]eventlog.Record{{ID: 1, Payload: []byte("old")}}); err != nil {
							t.Fatal(err)
						}
						if _, err := destination.Rewrite(t.Context(), 1, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
							t.Fatal(err)
						}
						expected = eventlog.ErrInvalid
					}
					if err := source.copyEventLog(ctx, observed); !errors.Is(err, expected) {
						t.Fatal(err, expected)
					}
					switch mode {
					case "empty", "canceled-before", "erased-destination":
						if observed.batches != 0 {
							t.Fatal("unexpected append", observed.batches)
						}
					default:
						if observed.batches != 1 {
							t.Fatal("expected one durable prefix", observed.batches)
						}
						// An incomplete copy must never become implicitly resumable.
						if err := source.copyEventLog(t.Context(), destination); !errors.Is(err, eventlog.ErrInvalid) {
							t.Fatal(err)
						}
					}
					if source.EventIndex() != head {
						t.Fatal("source frontier changed")
					}
				})
			}
		})
	}
}
