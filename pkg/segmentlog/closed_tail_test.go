package segmentlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRolloverRetainsAppendFileWithoutConversion(t *testing.T) {
	log := newLog(t, 32)
	if err := log.Append([]Record{{0, nil}, {10, nil}}); err != nil {
		t.Fatal(err)
	}
	before, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(log.path, before.Active.File)
	data := readBytes(t, path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append([]Record{{20, nil}}); err != nil {
		t.Fatal(err)
	}
	after, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Segments) != 1 {
		t.Fatal(after)
	}
	ref := after.Segments[0]
	if ref.File != before.Active.File || ref.TailBytes != int64(len(data)) || ref.Coverage != (Coverage{0, 11}) || ref.Count != 2 {
		t.Fatal(ref)
	}
	currentInfo, err := os.Stat(path)
	if err != nil || !os.SameFile(info, currentInfo) || !info.ModTime().Equal(currentInfo.ModTime()) || !bytes.Equal(data, readBytes(t, path)) {
		t.Fatal("rollover changed old file", err)
	}
	files, err := filepath.Glob(filepath.Join(log.path, "*.seg"))
	if err != nil || len(files) != 0 {
		t.Fatal("rollover created converted segment", files, err)
	}
	log = reopenLog(t, log)
	if r, err := log.Seek(1); err != nil || r.ID != 10 {
		t.Fatal(r, err)
	}
	if _, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil }); err != nil {
		t.Fatal(err)
	}
	noOp, err := log.catalog.Current()
	if err != nil || noOp.Segments[0] != ref || !bytes.Equal(data, readBytes(t, path)) {
		t.Fatal("no-op changed frozen file", err)
	}
	if _, err := log.Rewrite(t.Context(), 2, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 0, nil }); err != nil {
		t.Fatal(err)
	}
	rewritten, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	if rewritten.Segments[0].TailBytes != 0 || rewritten.Segments[0].Coverage != ref.Coverage {
		t.Fatal("replacement lost original range", rewritten)
	}
	if _, err := log.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("obsolete closed file retained", err)
	}
	log = reopenLog(t, log)
	for _, id := range []uint64{10, 20} {
		if _, err := log.Read(id); err != nil {
			t.Fatal(id, err)
		}
	}
	if _, err := log.Read(0); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestClosedTailReferenceValidation(t *testing.T) {
	log := newLog(t, 32)
	if err := log.Append([]Record{{0, nil}, {10, nil}, {20, nil}}); err != nil {
		t.Fatal(err)
	}
	c, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	ref := c.Segments[0]
	data := readBytes(t, filepath.Join(log.path, ref.File))
	for _, mode := range []string{"count", "size", "start", "end", "digest", "truncated", "extended"} {
		t.Run(mode, func(t *testing.T) {
			candidate := ref
			raw := bytes.Clone(data)
			switch mode {
			case "count":
				candidate.Count--
			case "size":
				candidate.TailBytes++
			case "start":
				candidate.Coverage.Start++
			case "end":
				candidate.Coverage.End = 10
			case "digest":
				candidate.SHA256[0] ^= 1
			case "truncated":
				raw = raw[:len(raw)-1]
			case "extended":
				raw = append(raw, 0)
			}
			if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), candidate); !errors.Is(err, ErrCorrupt) {
				t.Fatal("accepted invalid closed range", err)
			}
		})
	}
}
