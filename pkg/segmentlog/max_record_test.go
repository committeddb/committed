package segmentlog

import (
	"bytes"
	"errors"
	"testing"
)

// A record of MaxRecordBytes survives every path a record takes: an append
// that rolls the tail, sealing, compression, a sealed rewrite, scans and a
// reopen. One byte more is refused before anything is written.
func TestMaxRecordRoundTrip(t *testing.T) {
	l := newLog(t, 1<<20)
	big := bytes.Repeat([]byte("event payload "), MaxRecordBytes/14+1)[:MaxRecordBytes]
	if err := l.Append([]Record{{1, []byte("small")}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]Record{{2, big}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]Record{{3, make([]byte, MaxRecordBytes+1)}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("an oversized record must be refused", err)
	}
	if err := l.Append([]Record{{3, []byte("after")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Seal(); err != nil {
		t.Fatal(err)
	}
	for {
		did, err := l.CompressNextSealed()
		if err != nil {
			t.Fatal(err)
		}
		if !did {
			break
		}
	}
	check := func(l *Log) {
		t.Helper()
		got, err := l.Read(2)
		if err != nil || !bytes.Equal(got.Payload, big) {
			t.Fatal("the maximum record did not read back", err)
		}
		if got := scannedIDs(t, l); len(got) != 3 {
			t.Fatal(got)
		}
	}
	check(l)
	if _, err := l.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
		if r.ID == 1 {
			return nil, false, nil
		}
		return r.Payload, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := l.Read(2); err != nil || !bytes.Equal(got.Payload, big) {
		t.Fatal("the maximum record did not survive a rewrite", err)
	}
	l = reopenLog(t, l)
	if got, err := l.Read(2); err != nil || !bytes.Equal(got.Payload, big) {
		t.Fatal("the maximum record did not survive a reopen", err)
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}
