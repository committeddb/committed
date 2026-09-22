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
	transfer := LegacyTransfer{Log: log, Verify: func(raw []byte) error {
		reads++
		if fail && bytes.Equal(raw, frames[1]) {
			return errVerify
		}
		return nil
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
	layout, err := (LegacyTransfer{Log: log}).Layout()
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
