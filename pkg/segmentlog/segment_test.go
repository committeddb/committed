package segmentlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func sequence(records ...Record) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for _, rec := range records {
			if !yield(rec, nil) {
				return
			}
		}
	}
}

func encode(t testing.TB, records ...Record) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteSegment(&buf, Coverage{0, 1000}, sequence(records...), Options{BlockSize: 40}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func open(t testing.TB, data []byte) *Segment {
	t.Helper()
	s, err := OpenSegment(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func collect(t testing.TB, s *Segment) []Record {
	t.Helper()
	var records []Record
	for rec, err := range s.Records() {
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, rec)
	}
	return records
}

func TestSparseReads(t *testing.T) {
	records := []Record{{0, []byte("zero")}, {5, []byte("five")}, {250, []byte("large gap")}, {999, []byte("last")}}
	s := open(t, encode(t, records...))
	if s.Count() != 4 || s.Coverage() != (Coverage{0, 1000}) {
		t.Fatal("incorrect metadata")
	}
	if got := collect(t, s); !reflect.DeepEqual(got, records) {
		t.Fatalf("records: %v", got)
	}
	for _, tc := range []struct{ seek, want uint64 }{{0, 0}, {1, 5}, {5, 5}, {6, 250}, {251, 999}} {
		r, err := s.Seek(tc.seek)
		if err != nil || r.ID != tc.want {
			t.Fatalf("seek %d: %v, %v", tc.seek, r, err)
		}
	}
	for _, id := range []uint64{1, 249, 1000, ^uint64(0)} {
		if _, err := s.Read(id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("read %d: %v", id, err)
		}
	}
	if _, err := s.Seek(1000); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	r, _ := s.Read(0)
	r.Payload[0] = 'X'
	r, _ = s.Read(0)
	if string(r.Payload) != "zero" {
		t.Fatal("read leaked mutable shared buffer")
	}
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyAndOversizeBlock(t *testing.T) {
	empty := open(t, encode(t))
	if empty.Count() != 0 || len(collect(t, empty)) != 0 {
		t.Fatal("expected empty")
	}
	if _, err := empty.Seek(0); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("a"), 1000)
	s := open(t, encode(t, Record{10, payload}, Record{20, []byte("tail")}))
	r, err := s.Read(10)
	if err != nil || !bytes.Equal(r.Payload, payload) {
		t.Fatalf("oversize block: %v", err)
	}
}

func TestInvalidInput(t *testing.T) {
	for _, records := range [][]Record{
		{{ID: 1}, {ID: 1}}, {{ID: 2}, {ID: 1}}, {{ID: 1000}}, {{ID: 1, Payload: make([]byte, format.MaxPayload+1)}},
	} {
		if err := WriteSegment(io.Discard, Coverage{0, 1000}, sequence(records...), Options{}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if err := WriteSegment(io.Discard, Coverage{2, 2}, sequence(), Options{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, size := range []int{-1, 1, format.MaxBlock + 1} {
		if err := WriteSegment(io.Discard, Coverage{0, 10}, sequence(), Options{BlockSize: size}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestTruncationAndCorruption(t *testing.T) {
	data := encode(t, Record{1, []byte("one")}, Record{5, []byte("five")}, Record{20, []byte("twenty")})
	for length := 0; length < len(data); length++ {
		if _, err := OpenSegment(bytes.NewReader(data[:length]), int64(length)); err == nil {
			t.Fatalf("accepted truncation at %d", length)
		}
	}
	for i := range data {
		bad := bytes.Clone(data)
		bad[i] ^= 1
		s, err := OpenSegment(bytes.NewReader(bad), int64(len(bad)))
		if err == nil {
			err = s.Verify()
		}
		if err == nil {
			t.Fatalf("accepted corruption at %d", i)
		}
	}
}

func TestUnsupportedAndMalformedIndex(t *testing.T) {
	data := encode(t, Record{1, []byte("one")})
	for _, offset := range []int{8, 10} {
		bad := bytes.Clone(data)
		bad[offset] = 99
		format.LE.PutUint32(bad[28:], format.CRC(bad[:28]))
		if _, err := OpenSegment(bytes.NewReader(bad), int64(len(bad))); !errors.Is(err, ErrUnsupported) {
			t.Fatal(err)
		}
	}
	// Recompute checksums to test structural checks, not just bit-flip detection.
	for _, mutate := range []func([]byte){
		func(e []byte) { format.LE.PutUint64(e[16:], ^uint64(0)) },
		func(e []byte) { format.LE.PutUint32(e[24:], ^uint32(0)) },
		func(e []byte) { format.LE.PutUint32(e[28:], 0) },
		func(e []byte) { format.LE.PutUint64(e[8:], 1000) },
	} {
		bad := bytes.Clone(data)
		footer := bad[len(bad)-format.FooterSize:]
		index := bad[int(format.LE.Uint64(footer)) : len(bad)-format.FooterSize]
		mutate(index)
		format.LE.PutUint32(footer[20:], format.CRC(index))
		format.LE.PutUint32(footer[28:], format.CRC(footer[:28]))
		if _, err := OpenSegment(bytes.NewReader(bad), int64(len(bad))); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}

func TestSelectiveRewrite(t *testing.T) {
	for _, mode := range []string{"noop", "partial", "all", "inplace"} {
		t.Run(mode, func(t *testing.T) {
			records := []Record{{1, []byte("one")}, {10, []byte("secret public")}, {20, []byte("three")}, {50, []byte("last")}}
			data := encode(t, records...)
			source := bytes.Clone(data)
			s := open(t, data)
			calls, creates := map[uint64]int{}, 0
			var out bytes.Buffer
			changed, err := s.Rewrite(context.Background(), func() (io.Writer, error) { creates++; return &out, nil }, func(r Record) ([]byte, bool, error) {
				calls[r.ID]++
				switch mode {
				case "all":
					return nil, false, nil
				case "partial":
					if r.ID == 10 {
						return []byte("public"), true, nil
					}
					if r.ID == 20 {
						return nil, false, nil
					}
				case "inplace":
					if r.ID == 10 {
						r.Payload[0] = 'X'
					}
				}
				return r.Payload, true, nil
			}, Options{BlockSize: 40})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range records {
				if calls[r.ID] != 1 {
					t.Fatalf("callback count: %v", calls)
				}
			}
			if !bytes.Equal(data, source) {
				t.Fatal("source mutated")
			}
			if mode == "noop" {
				if changed || creates != 0 || out.Len() != 0 {
					t.Fatal("no-op produced output")
				}
				return
			}
			if !changed || creates != 1 {
				t.Fatal("missing replacement")
			}
			replacement := open(t, out.Bytes())
			if replacement.Coverage() != s.Coverage() {
				t.Fatal("coverage shifted")
			}
			want := records
			switch mode {
			case "all":
				want = nil
			case "partial":
				want = []Record{records[0], {10, []byte("public")}, records[3]}
			case "inplace":
				want[1].Payload = []byte("Xecret public")
			}
			if got := collect(t, replacement); !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
}

func TestRewritePreservesUnchangedFiles(t *testing.T) {
	dir := t.TempDir()
	for i := range 3 {
		path := filepath.Join(dir, fmt.Sprintf("%d.seg", i))
		data := encode(t, Record{uint64(i*10 + 1), []byte("keep")}, Record{uint64(i*10 + 2), []byte("erase")})
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		s, err := OpenSegment(f, before.Size())
		if err != nil {
			t.Fatal(err)
		}
		var output *os.File
		changed, err := s.Rewrite(context.Background(), func() (io.Writer, error) {
			var err error
			output, err = os.OpenFile(path+".replacement", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			return output, err
		}, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 12, nil }, Options{})
		if closeErr := f.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if output != nil {
			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err != nil || changed != (i == 1) {
			t.Fatalf("rewrite %d: %v %v", i, changed, err)
		}
		after, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) || before.ModTime() != after.ModTime() || sha256.Sum256(data) != sha256.Sum256(got) {
			t.Fatal("original file changed")
		}
		if i != 1 {
			if _, err := os.Stat(path + ".replacement"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unaffected file replaced")
			}
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }

type failWriter struct {
	left int
	err  error
}

func (w *failWriter) Write(b []byte) (int, error) {
	if len(b) > w.left {
		n := w.left
		w.left = 0
		return n, w.err
	}
	w.left -= len(b)
	return len(b), nil
}

func TestFailuresAndCancellation(t *testing.T) {
	boom := errors.New("injected failure")
	records := []Record{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}}
	data := encode(t, records...)
	s := open(t, data)
	if err := WriteSegment(shortWriter{}, Coverage{0, 1000}, sequence(records...), Options{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	for limit := 0; limit < len(data); limit++ {
		err := WriteSegment(&failWriter{limit, boom}, Coverage{0, 1000}, sequence(records...), Options{BlockSize: 40})
		if !errors.Is(err, boom) {
			t.Fatalf("write failure at %d: %v", limit, err)
		}
	}
	for _, at := range []uint64{1, 3} {
		_, err := s.Rewrite(context.Background(), func() (io.Writer, error) { return io.Discard, nil }, func(r Record) ([]byte, bool, error) {
			if r.ID == at {
				return nil, false, boom
			}
			return nil, false, nil
		}, Options{})
		if !errors.Is(err, boom) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Rewrite(ctx, func() (io.Writer, error) { t.Fatal("created on cancellation"); return nil, nil }, func(r Record) ([]byte, bool, error) { t.Fatal("transformed on cancellation"); return nil, false, nil }, Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	_, err = s.Rewrite(ctx, func() (io.Writer, error) { return io.Discard, nil }, func(r Record) ([]byte, bool, error) {
		if r.ID == 3 {
			cancel()
		}
		return nil, false, nil
	}, Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, err = s.Rewrite(context.Background(), func() (io.Writer, error) { return nil, boom }, func(r Record) ([]byte, bool, error) { return nil, false, nil }, Options{})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	broken := bytes.Clone(data)
	broken[format.HeaderSize+12] ^= 1
	s = open(t, broken)
	_, err = s.Rewrite(context.Background(), func() (io.Writer, error) { t.Fatal("created for corrupt source"); return nil, nil }, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil }, Options{})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestConcurrentReads(t *testing.T) {
	s := open(t, encode(t, Record{10, []byte("ten")}, Record{100, []byte("hundred")}))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 20 {
				r, err := s.Seek(11)
				if err != nil || r.ID != 100 {
					t.Error("concurrent seek", err)
				}
				if err := s.Verify(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
}

func FuzzSegment(f *testing.F) {
	f.Add(encode(f, Record{0, []byte("zero")}, Record{50, []byte("fifty")}))
	f.Add(encode(f))
	f.Add(compressed(f, ZstdDefault, Record{1, bytes.Repeat([]byte("payload"), 100)}))
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := OpenSegment(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return
		}
		if err := s.Verify(); err != nil {
			return
		}
		var count uint64
		for rec, err := range s.Records() {
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.Read(rec.ID)
			if err != nil || !bytes.Equal(got.Payload, rec.Payload) {
				t.Fatal("lookup disagrees with scan")
			}
			count++
		}
		if count != s.Count() {
			t.Fatal("count disagrees with scan")
		}
	})
}
