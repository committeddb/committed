package wal

import (
	"errors"
	"reflect"
	"testing"

	native "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func TestEntryPrefixScanBackends(t *testing.T) {
	backends := map[string]func(*testing.T) entryCursor{
		"production-tidwall": func(t *testing.T) entryCursor {
			log, err := native.Open(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			for i, id := range []uint64{10, 30, 90} {
				if err := log.Write(uint64(i+1), frame(experimentEntry(t, id, pb.EntryNormal))); err != nil {
					t.Fatal(err)
				}
			}
			return newLegacyEntryCursor(&Storage{eventLog: tidwallbackend.OwnLegacy(log)}, 0)
		},
	}
	for name, open := range eventLogTestBackends() {
		backends[name] = func(t *testing.T) entryCursor {
			log, err := open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			for _, id := range []uint64{10, 30, 90} {
				if err := log.Append([]eventlog.Record{{ID: id, Payload: experimentEntry(t, id, pb.EntryNormal)}}); err != nil {
					t.Fatal(err)
				}
			}
			return newEventEntryCursor(log.NewCursor(), 0)
		}
	}
	for name, open := range backends {
		t.Run(name, func(t *testing.T) {
			cursor := open(t)
			defer func() { _ = cursor.Close() }()
			for _, tc := range []struct {
				bound uint64
				want  []uint64
			}{
				{0, nil},
				{9, nil},
				{10, []uint64{10}},
				{29, []uint64{10}},
				{30, []uint64{10, 30}},
				{^uint64(0), []uint64{10, 30, 90}},
			} {
				var got []uint64
				err := scanEntryPrefix(cursor, tc.bound, func(e *pb.Entry) error {
					got = append(got, e.GetIndex())
					return nil
				})
				if err != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatal(tc.bound, got, err)
				}
			}
			errVisit := errors.New("stop selection")
			calls := 0
			err := scanEntryPrefix(cursor, 90, func(e *pb.Entry) error {
				calls++
				return errVisit
			})
			if !errors.Is(err, errVisit) || calls != 1 {
				t.Fatal(calls, err)
			}
			if err := scanEntryPrefix(cursor, 90, nil); !errors.Is(err, eventlog.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
}

func TestEntryPrefixScanSurfacesCorruption(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if err := log.Write(1, frame(experimentEntry(t, 10, pb.EntryNormal))); err != nil {
		t.Fatal(err)
	}
	// The first position is valid; the next sequential read must fail loudly.
	if err := log.Write(2, frame([]byte{0xff})); err != nil {
		t.Fatal(err)
	}
	if err := log.Write(3, frame(experimentEntry(t, 90, pb.EntryNormal))); err != nil {
		t.Fatal(err)
	}
	s := &Storage{eventLog: tidwallbackend.OwnLegacy(log)}
	var got []uint64
	err = s.scanEventEntries(90, func(e *pb.Entry) error {
		got = append(got, e.GetIndex())
		return nil
	})
	if err == nil || !reflect.DeepEqual(got, []uint64{10}) {
		t.Fatal(got, err)
	}
	// The exact prefix does not need to read a corrupt record beyond its bound.
	if err := s.scanEventEntries(10, func(*pb.Entry) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
