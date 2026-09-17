package eventlog_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type backend struct {
	name   string
	create func(string) (eventlog.EventLog, error)
	open   func(string) (eventlog.EventLog, error)
}

func backends() []backend {
	return backendsWithSegmentBytes(128)
}

func backendsWithSegmentBytes(segmentBytes int) []backend {
	result := make([]backend, 0, 6)
	for _, compressed := range []bool{false, true} {
		name := "plain"
		codec := segmentlog.NoCompression
		if compressed {
			name = "zstd"
			codec = segmentlog.ZstdDefault
		}
		result = append(result, backend{"segmented-bbolt/" + name, func(path string) (eventlog.EventLog, error) {
			return segmented.CreateBolt(path, 0, segmentlog.LogOptions{SegmentBytes: segmentBytes, Encoding: segmentlog.Options{Compression: codec}})
		}, func(path string) (eventlog.EventLog, error) {
			return segmented.OpenBolt(path, segmentlog.Options{Compression: codec})
		}})
		result = append(result, backend{"segmented/" + name, func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 0, segmentlog.LogOptions{SegmentBytes: segmentBytes, Encoding: segmentlog.Options{Compression: codec}})
		}, func(path string) (eventlog.EventLog, error) {
			return segmented.Open(path, segmentlog.Options{Compression: codec})
		}}, backend{"tidwall/" + name, func(path string) (eventlog.EventLog, error) {
			return tidwall.Create(path, 0, tidwall.Options{SegmentBytes: segmentBytes, Compress: compressed})
		}, func(path string) (eventlog.EventLog, error) { return tidwall.Open(path) }})
	}
	return result
}

func TestEventLogConformance(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			path := t.TempDir()
			log, e := b.create(path)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = log.Close() })
			if _, e := b.open(path); !errors.Is(e, eventlog.ErrLocked) {
				t.Fatal("competing ownership", e)
			}
			if id, ok, e := log.LastAppended(); e != nil || ok || id != 0 {
				t.Fatal(id, ok, e)
			}
			records := []eventlog.Record{{ID: 0, Payload: []byte("zero")}, {ID: 10, Payload: bytes.Repeat([]byte("a"), 100)}, {ID: 100, Payload: []byte("last")}}
			if e := log.Append(records); e != nil {
				t.Fatal(e)
			}
			if e := log.Append([]eventlog.Record{{ID: 101}, {ID: 100}}); !errors.Is(e, eventlog.ErrInvalid) {
				t.Fatal(e)
			}
			if _, e := log.Read(101); !errors.Is(e, eventlog.ErrNotFound) {
				t.Fatal("partial invalid batch", e)
			}
			r, e := log.Seek(1)
			if e != nil || r.ID != 10 {
				t.Fatal(r, e)
			}
			r.Payload[0] = 'X'
			r, e = log.Read(10)
			if e != nil || r.Payload[0] != 'a' {
				t.Fatal("aliased payload", e)
			}
			var ids []uint64
			if e := log.Scan(t.Context(), eventlog.Coverage{Start: 1, End: 100}, func(r eventlog.Record) error { ids = append(ids, r.ID); return nil }); e != nil || !reflect.DeepEqual(ids, []uint64{10}) {
				t.Fatal(ids, e)
			}
			boom := errors.New("stop scan")
			if e := log.Scan(t.Context(), eventlog.Coverage{End: 101}, func(eventlog.Record) error { return boom }); !errors.Is(e, boom) {
				t.Fatal(e)
			}
			calls := map[uint64]int{}
			result, e := log.Rewrite(t.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) {
				calls[r.ID]++
				if r.ID == 10 {
					r.Payload[0] = 'b'
				}
				return r.Payload, r.ID != 100, nil
			})
			if e != nil || !result.Published || result.ChangedRecords != 2 || len(calls) != 3 {
				t.Fatal(result, calls, e)
			}
			for _, n := range calls {
				if n != 1 {
					t.Fatal("repeated transform", calls)
				}
			}
			if e := log.Close(); e != nil {
				t.Fatal(e)
			}
			log, e = b.open(path)
			if e != nil {
				t.Fatal(e)
			}
			if id, ok, e := log.LastAppended(); e != nil || !ok || id != 100 {
				t.Fatal("lost erased frontier", id, ok, e)
			}
			if _, e := log.Read(100); !errors.Is(e, eventlog.ErrNotFound) {
				t.Fatal(e)
			}
			r, e = log.Read(10)
			if e != nil || r.Payload[0] != 'b' {
				t.Fatal(r, e)
			}
			if e := log.Append([]eventlog.Record{{ID: 100}}); !errors.Is(e, eventlog.ErrInvalid) {
				t.Fatal(e)
			}
			if e := log.Append([]eventlog.Record{{ID: 150}}); e != nil {
				t.Fatal(e)
			}
			result, e = log.Rewrite(t.Context(), 2, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
			if e != nil || !result.Published {
				t.Fatal(result, e)
			}
			if _, e := log.Reclaim(t.Context()); e != nil {
				t.Fatal(e)
			}
			if e := log.Close(); e != nil {
				t.Fatal(e)
			}
			log, e = b.open(path)
			if e != nil {
				t.Fatal(e)
			}
			if id, ok, e := log.LastAppended(); e != nil || !ok || id != 150 {
				t.Fatal(id, ok, e)
			}
			if _, e := log.Seek(0); !errors.Is(e, eventlog.ErrNotFound) {
				t.Fatal(e)
			}
			result, e = log.Rewrite(t.Context(), 3, func(eventlog.Record) ([]byte, bool, error) { t.Fatal("empty log callback"); return nil, false, nil })
			if e != nil || !result.Published || result.ChangedRecords != 0 {
				t.Fatal(result, e)
			}
			if _, e := log.Rewrite(t.Context(), 3, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, true, nil }); !errors.Is(e, eventlog.ErrInvalid) {
				t.Fatal(e)
			}
			if e := log.Append([]eventlog.Record{{ID: 200, Payload: []byte("new")}}); e != nil {
				t.Fatal(e)
			}
			if e := log.Close(); e != nil {
				t.Fatal(e)
			}
			if e := log.Close(); e != nil {
				t.Fatal(e)
			}
			if _, _, e := log.LastAppended(); !errors.Is(e, eventlog.ErrClosed) {
				t.Fatal(e)
			}
		})
	}
}

func TestEventLogRewriteFailureRecovery(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			path := t.TempDir()
			log, e := b.create(path)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = log.Close() })
			if e := log.Append([]eventlog.Record{{ID: 1, Payload: []byte("first")}, {ID: 2, Payload: []byte("second")}}); e != nil {
				t.Fatal(e)
			}
			boom := errors.New("callback failed")
			result, e := log.Rewrite(t.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) {
				if r.ID == 2 {
					return nil, false, boom
				}
				return []byte("changed"), true, nil
			})
			if !errors.Is(e, boom) || !errors.Is(e, eventlog.ErrPoisoned) || result.Published {
				t.Fatal(result, e)
			}
			if _, e := log.Read(1); !errors.Is(e, eventlog.ErrPoisoned) {
				t.Fatal(e)
			}
			if e := log.Close(); e != nil {
				t.Fatal(e)
			}
			log, e = b.open(path)
			if e != nil {
				t.Fatal(e)
			}
			r, e := log.Read(1)
			if e != nil || string(r.Payload) != "first" {
				t.Fatal("partial publication", r, e)
			}
			if _, e := log.Reclaim(t.Context()); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, e := log.Rewrite(ctx, 1, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, true, nil }); !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			result, e = log.Rewrite(t.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, true, nil })
			if e != nil || !result.Published || result.ChangedRecords != 0 {
				t.Fatal(result, e)
			}
		})
	}
}
