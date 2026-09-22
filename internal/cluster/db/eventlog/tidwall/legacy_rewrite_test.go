package tidwall

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyRewritePreservesSurvivorBytes(t *testing.T) {
	path := t.TempDir()
	writer, err := CreateLegacyRewrite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	frames := [][]byte{[]byte("first framed survivor"), []byte("second framed survivor"), []byte("locked delta")}
	if writer.Count() != 0 {
		t.Fatal(writer.Count())
	}
	for _, frame := range frames[:2] {
		if err := writer.Append(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompressSealed(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(frames[2]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if writer.Count() != 3 {
		t.Fatal(writer.Count())
	}
	if err := writer.Append([]byte("after close")); err == nil || writer.Count() != 3 {
		t.Fatal("failed append advanced survivor count", err)
	}
	reopened, err := native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	first, err := reopened.FirstIndex()
	if err != nil || first != 1 {
		t.Fatal(first, err)
	}
	last, err := reopened.LastIndex()
	if err != nil || last != 3 {
		t.Fatal(last, err)
	}
	for i, want := range frames {
		got, err := reopened.Read(uint64(i + 1))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("rewrite changed survivor bytes", err)
		}
	}
}

func TestLegacyRewriteRejectsExistingHistory(t *testing.T) {
	path := t.TempDir()
	log, err := native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Write(1, []byte("existing")); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if writer, err := CreateLegacyRewrite(path); !errors.Is(err, eventlog.ErrInvalid) || writer != nil {
		t.Fatal(writer, err)
	}
	log, err = native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if got, err := log.Read(1); err != nil || string(got) != "existing" {
		t.Fatal("rejected creation changed existing history", err)
	}
}

func TestLegacyRewriteCopyRange(t *testing.T) {
	path := t.TempDir()
	writer, err := CreateLegacyRewrite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	var reads, transforms []uint64
	read := func(seq uint64) ([]byte, error) {
		reads = append(reads, seq)
		return binary.BigEndian.AppendUint64(nil, seq), nil
	}
	transform := func(raw []byte) ([]byte, bool, error) {
		seq := binary.BigEndian.Uint64(raw)
		transforms = append(transforms, seq)
		return raw, seq != 20, nil
	}
	if err := writer.CopyRange(19, 21, read, transform); err != nil {
		t.Fatal(err)
	}
	// A later catch-up range appends after the already-written survivors.
	if err := writer.CopyRange(22, 22, read, transform); err != nil {
		t.Fatal(err)
	}
	if err := writer.CopyRange(2, 1, read, transform); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(reads, []uint64{19, 20, 21, 22}) || !slices.Equal(reads, transforms) || writer.Count() != 3 {
		t.Fatal(reads, transforms, writer.Count())
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	for i, want := range []uint64{19, 21, 22} {
		raw, err := log.Read(uint64(i + 1))
		if err != nil || binary.BigEndian.Uint64(raw) != want {
			t.Fatal(i, err)
		}
	}
}

func TestLegacyRewriteCopyRangeStopsOnFailure(t *testing.T) {
	errInjected := errors.New("injected failure")
	for _, stage := range []string{"read", "transform", "write"} {
		t.Run(stage, func(t *testing.T) {
			writer, err := CreateLegacyRewrite(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = writer.Close() }()
			var reads, transforms int
			read := func(seq uint64) ([]byte, error) {
				reads++
				if seq == 2 && stage == "read" {
					return nil, errInjected
				}
				return binary.BigEndian.AppendUint64(nil, seq), nil
			}
			transform := func(raw []byte) ([]byte, bool, error) {
				transforms++
				if binary.BigEndian.Uint64(raw) == 2 && stage == "transform" {
					return nil, false, errInjected
				}
				return raw, true, nil
			}
			if stage == "write" {
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err = writer.CopyRange(1, 3, read, transform)
			if stage == "write" {
				if err == nil || reads != 1 || transforms != 1 || writer.Count() != 0 {
					t.Fatal(reads, transforms, writer.Count(), err)
				}
			} else {
				wantTransforms := 2
				if stage == "read" {
					wantTransforms = 1
				}
				if !errors.Is(err, errInjected) || reads != 2 || transforms != wantTransforms || writer.Count() != 1 {
					t.Fatal(reads, transforms, writer.Count(), err)
				}
			}
		})
	}
}
