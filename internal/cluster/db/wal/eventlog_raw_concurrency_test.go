package wal

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestEventLogRawReadsAndScanDuringRewrite(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			opts := segmentlog.LogOptions{SegmentBytes: 16}
			if cached {
				opts.Cache = segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}
			}
			log, err := segmented.Create(t.TempDir(), 1, opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			adapter := &eventLogAdapter{log: log}
			first := experimentEntry(t, 10, pb.EntryNormal, experimentRow("one", "value"))
			second := experimentEntry(t, 20, pb.EntryNormal, experimentRow("two", "value"))
			if err := adapter.appendRaw([][]byte{first, second}); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			done := make(chan error, 1)
			go func() {
				_, err := adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) {
					if bytes.Equal(raw, first) {
						close(entered)
						<-release
						return false, nil, nil
					}
					return true, raw, nil
				})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("rewrite did not pause")
			}
			scanEntered, scanRelease := make(chan struct{}), make(chan struct{})
			unblockScan := sync.OnceFunc(func() { close(scanRelease) })
			defer unblockScan()
			reads := make(chan error, 1)
			go func() {
				raw, err := adapter.readRaw(10)
				if err != nil {
					reads <- err
					return
				}
				if !bytes.Equal(raw, first) {
					reads <- errors.New("unpublished replacement visible")
					return
				}
				raw[0] ^= 0xff // caller-owned bytes must not change another lookup
				id, raw, err := adapter.seekRaw(1)
				if err == nil && (id != 10 || !bytes.Equal(raw, first)) {
					err = errors.New("raw seek lost original contents")
				}
				if err == nil {
					raw, err = adapter.readRaw(20)
					if err == nil && !bytes.Equal(raw, second) {
						err = errors.New("tail changed during preparation")
					}
				}
				if err == nil {
					count := 0
					err = adapter.scanRaw(t.Context(), eventlog.Coverage{Start: 1, End: 21}, func(id uint64, raw []byte) error {
						count++
						if count == 1 {
							close(scanEntered)
							<-scanRelease
						}
						wantID, want := uint64(10), first
						if count == 2 {
							wantID, want = 20, second
						}
						if count > 2 || id != wantID || !bytes.Equal(raw, want) {
							return errors.New("scan mixed rewrite generations")
						}
						raw[0] ^= 0xff
						return nil
					})
					if err == nil && count != 2 {
						err = errors.New("scan lost original records")
					}
				}
				reads <- err
			}()
			wait := func(ch <-chan error) {
				t.Helper()
				select {
				case err := <-ch:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("operation blocked during rewrite")
				}
			}
			select {
			case <-scanEntered:
			case err := <-reads:
				t.Fatalf("reads stopped before scan: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("scan blocked during preparation")
			}
			// Let rewrite preparation resume while the scan owns the old view.
			// Publication must wait for the entire scan callback lifetime.
			unblock()
			select {
			case err := <-done:
				t.Fatalf("rewrite finished during scan: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			unblockScan()
			wait(reads)
			wait(done)
			if _, err := adapter.readRaw(10); !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatal("erased record remains visible", err)
			}
			id, raw, err := adapter.seekRaw(1)
			if err != nil || id != 20 || !bytes.Equal(raw, second) {
				t.Fatal("lookup did not observe publication", id, err)
			}
		})
	}
}
