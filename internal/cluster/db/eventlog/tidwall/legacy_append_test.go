package tidwall

import (
	"encoding/binary"
	"errors"
	"testing"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyAppenderValidatesBeforeWriting(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	failure := errors.New("encode failed")
	codec := LegacyCodec{
		Encode: func(r eventlog.Record) ([]byte, error) {
			if string(r.Payload) == "fail" {
				return nil, failure
			}
			return binary.BigEndian.AppendUint64(nil, r.ID), nil
		},
		Decode: func(raw []byte) (eventlog.Record, error) {
			if len(raw) != 8 {
				return eventlog.Record{}, eventlog.ErrCorrupt
			}
			return eventlog.Record{ID: binary.BigEndian.Uint64(raw)}, nil
		},
	}
	appender := NewLegacyAppender(log, codec)
	if err := appender.Append([]eventlog.Record{{ID: 10}}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		records []eventlog.Record
		want    error
	}{
		{[]eventlog.Record{{ID: 20}, {ID: 19}}, eventlog.ErrInvalid},
		{[]eventlog.Record{{ID: 10}}, eventlog.ErrInvalid},
		{[]eventlog.Record{{ID: 20}, {ID: 30, Payload: []byte("fail")}}, failure},
	} {
		if err := appender.Append(tc.records); !errors.Is(err, tc.want) {
			t.Fatal(err)
		}
		if last, err := log.LastIndex(); err != nil || last != 1 {
			t.Fatal("invalid batch wrote a prefix", last, err)
		}
	}
	// A native append performed by the owner invalidates cached progress.
	if err := log.Write(2, binary.BigEndian.AppendUint64(nil, 30)); err != nil {
		t.Fatal(err)
	}
	if err := appender.Append([]eventlog.Record{{ID: 20}}); !errors.Is(err, eventlog.ErrInvalid) {
		t.Fatal("cached progress ignored native append", err)
	}
	if err := appender.Append([]eventlog.Record{{ID: 40}}); err != nil {
		t.Fatal(err)
	}
	if last, err := log.LastIndex(); err != nil || last != 3 {
		t.Fatal(last, err)
	}
}

func TestLegacyAppenderReportsLogicalProgressWithoutDuplicateDecode(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	decodes := 0
	appender := NewLegacyAppender(log, LegacyCodec{
		Encode: func(r eventlog.Record) ([]byte, error) { return binary.BigEndian.AppendUint64(nil, r.ID), nil },
		Decode: func(raw []byte) (eventlog.Record, error) {
			decodes++
			return eventlog.Record{ID: binary.BigEndian.Uint64(raw)}, nil
		},
	})
	check := func(want uint64, has bool) {
		t.Helper()
		id, ok, err := appender.LastAppended()
		if err != nil || id != want || ok != has {
			t.Fatal(id, ok, err)
		}
	}
	check(0, false)
	// Zero is a real logical ID and must not be confused with empty history.
	if err := log.Write(1, binary.BigEndian.AppendUint64(nil, 0)); err != nil {
		t.Fatal(err)
	}
	check(0, true)
	check(0, true)
	if decodes != 1 {
		t.Fatal("progress redecoded unchanged tail", decodes)
	}
	if err := appender.Append([]eventlog.Record{{ID: 10}}); err != nil {
		t.Fatal(err)
	}
	check(10, true)
	if decodes != 1 {
		t.Fatal("append redecoded observed frontier", decodes)
	}
	// The production fetch path can extend the same handle through another
	// appender. Cached progress must refresh exactly once before the next write.
	if err := log.Write(3, binary.BigEndian.AppendUint64(nil, 30)); err != nil {
		t.Fatal(err)
	}
	check(30, true)
	if err := appender.Append([]eventlog.Record{{ID: 40}}); err != nil {
		t.Fatal(err)
	}
	check(40, true)
	if decodes != 2 {
		t.Fatal("external append frontier decoded more than once", decodes)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := appender.LastAppended(); err == nil {
		t.Fatal("cached progress hid closed handle")
	}
}
