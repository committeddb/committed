package tidwall

import (
	"encoding/binary"
	"errors"
	"testing"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyLookupSparseIDsAndOwnership(t *testing.T) {
	opts := *native.DefaultOptions
	opts.NoCopy = true
	log, err := native.Open(t.TempDir(), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	lookup := NewLegacyLookup(log, func(raw []byte) (eventlog.Record, error) {
		if len(raw) < 8 {
			return eventlog.Record{}, eventlog.ErrCorrupt
		}
		return eventlog.Record{ID: binary.BigEndian.Uint64(raw), Payload: raw[8:]}, nil
	})
	if _, err := lookup.Read(10); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	for i, id := range []uint64{10, 30, 90} {
		raw := append(binary.BigEndian.AppendUint64(nil, id), "payload"...)
		if err := log.Write(uint64(i+1), raw); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{0, 1, 9, 11, 29, 31, 91, ^uint64(0)} {
		if _, err := lookup.Read(id); !errors.Is(err, eventlog.ErrNotFound) {
			t.Fatal(id, err)
		}
	}
	for _, id := range []uint64{90, 10, 30, 10} {
		record, err := lookup.Read(id)
		if err != nil || record.ID != id || string(record.Payload) != "payload" {
			t.Fatal(record, err)
		}
		record.Payload[0] = 'X'
		again, err := lookup.Read(id)
		if err != nil || string(again.Payload) != "payload" {
			t.Fatal("caller mutation reached native storage", again, err)
		}
	}
	if err := log.TruncateFront(2); err != nil {
		t.Fatal(err)
	}
	if _, err := lookup.Read(10); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal("lookup ignored truncated prefix", err)
	}
	if r, err := lookup.Read(30); err != nil || r.ID != 30 {
		t.Fatal(r, err)
	}
	// A decode error is not absence, including on a search probe.
	if err := log.Write(4, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := lookup.Read(100); !errors.Is(err, eventlog.ErrCorrupt) {
		t.Fatal("decode failure hidden", err)
	}
}
