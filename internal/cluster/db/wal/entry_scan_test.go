package wal

import (
	"errors"
	"reflect"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestEntryPrefixScanBackends(t *testing.T) {
	backends := map[string]func(*testing.T) entryCursor{}
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
	// The first position is valid; the next sequential read must fail loudly.
	s := &Storage{eventLog: productionBindingOf(t,
		eventlog.Record{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal)},
		eventlog.Record{ID: 20, Payload: []byte{0xff}},
		eventlog.Record{ID: 90, Payload: experimentEntry(t, 90, pb.EntryNormal)})}
	var got []uint64
	err := s.scanEventEntries(90, func(e *pb.Entry) error {
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
