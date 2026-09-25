package tidwall

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	native "github.com/tidwall/wal"
)

func TestLegacyTransferPreservesFramesAndBudget(t *testing.T) {
	opts := *native.DefaultOptions
	opts.NoCopy = true
	log, err := native.Open(t.TempDir(), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	frames := [][]byte{[]byte("first-frame"), []byte("second-frame"), []byte("third-frame")}
	for i, raw := range frames {
		if err := log.Write(uint64(i+1), raw); err != nil {
			t.Fatal(err)
		}
	}
	errVerify := errors.New("bad frame")
	fail := false
	reads := 0
	transfer := LegacyTransfer{log: log, DecodeFrame: func(raw []byte) ([]byte, error) {
		reads++
		if fail && bytes.Equal(raw, frames[1]) {
			return nil, errVerify
		}
		return raw, nil
	}}
	data, last, err := transfer.EncodeRecords(1, 3, 1)
	want := binary.AppendUvarint(nil, uint64(len(frames[0])))
	want = append(want, frames[0]...)
	if err != nil || last != 1 || reads != 1 || !bytes.Equal(data, want) {
		t.Fatal(data, last, reads, err)
	}
	data[1] = 'X'
	raw, err := transfer.Read(1)
	if err != nil || !bytes.Equal(raw, frames[0]) {
		t.Fatal("encoded stream aliases native bytes", err)
	}
	var all []byte
	for _, raw := range frames {
		all = binary.AppendUvarint(all, uint64(len(raw)))
		all = append(all, raw...)
	}
	data, last, err = transfer.EncodeRecords(1, 3, 1000)
	if err != nil || last != 3 || !bytes.Equal(data, all) {
		t.Fatal(last, err)
	}
	data, last, err = transfer.EncodeRecords(3, 2, 1000)
	if err != nil || last != 0 || len(data) != 0 {
		t.Fatal(last, err)
	}
	fail = true
	data, last, err = transfer.EncodeRecords(1, 3, 1000)
	if !errors.Is(err, errVerify) || last != 0 || data != nil {
		t.Fatal("exposed partially verified stream", last, err)
	}
}

func TestLegacyTransferLayoutMatchesNative(t *testing.T) {
	opts := *native.DefaultOptions
	opts.SegmentSize = 32
	log, err := native.Open(t.TempDir(), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	for i := uint64(1); i <= 8; i++ {
		if err := log.Write(i, bytes.Repeat([]byte("x"), 20)); err != nil {
			t.Fatal(err)
		}
	}
	nativeLayout, err := log.LayoutSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	layout, err := (LegacyTransfer{log: log}).Layout()
	if err != nil {
		t.Fatal(err)
	}
	if layout.LastSeq != nativeLayout.LastIndex || layout.TailPath != nativeLayout.Tail.Path || layout.TailLen != nativeLayout.TailLen || layout.TailFirstSeq != nativeLayout.Tail.Index || len(layout.Sealed) != len(nativeLayout.Sealed) {
		t.Fatal("layout changed")
	}
	for i, s := range layout.Sealed {
		want := nativeLayout.Sealed[i]
		if s.Path != want.Path || s.FirstSeq != want.Index || s.Compressed != native.IsCompressedSegmentPath(want.Path) {
			t.Fatal(s, want)
		}
	}
}

func TestLegacyTransferPayloadAndNativeBounds(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	calls := 0
	errFrame := errors.New("bad envelope")
	access := LegacyTransfer{log: log, DecodeFrame: func(raw []byte) ([]byte, error) {
		calls++
		if len(raw) == 0 || raw[0] != '!' {
			return nil, errFrame
		}
		return raw[1:], nil
	}}
	checkBounds := func(wantFirst, wantLast uint64) {
		t.Helper()
		first, err := access.FirstSequence()
		if err != nil || first != wantFirst {
			t.Fatal(first, err)
		}
		last, err := access.LastSequence()
		if err != nil || last != wantLast {
			t.Fatal(last, err)
		}
	}
	checkBounds(0, 0)
	if err := log.Write(1, []byte("!payload")); err != nil {
		t.Fatal(err)
	}
	if err := log.Write(2, []byte("!next")); err != nil {
		t.Fatal(err)
	}
	checkBounds(1, 2)
	payload, err := access.ReadPayload(1)
	if err != nil || string(payload) != "payload" || calls != 1 {
		t.Fatal(payload, calls, err)
	}
	frame, err := access.Read(1)
	if err != nil || string(frame) != "!payload" || calls != 2 {
		t.Fatal(frame, calls, err)
	}
	if err := log.TruncateFront(2); err != nil {
		t.Fatal(err)
	}
	checkBounds(2, 2)
	if err := log.Write(3, []byte("invalid")); err != nil {
		t.Fatal(err)
	}
	if _, err := access.ReadPayload(3); !errors.Is(err, errFrame) || calls != 3 {
		t.Fatal(calls, err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := access.FirstSequence(); err == nil {
		t.Fatal("closed bounds accepted")
	}
	if _, err := access.LastSequence(); err == nil {
		t.Fatal("closed bounds accepted")
	}
}
