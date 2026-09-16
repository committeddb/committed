package segmentlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func tailFile(t testing.TB, start uint64) *os.File {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "tail.active"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if err := WriteTailHeader(f, start); err != nil {
		t.Fatal(err)
	}
	return f
}
func tailBytes(t testing.TB, f *os.File) []byte {
	t.Helper()
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTailAppendReopenAndSeal(t *testing.T) {
	f := tailFile(t, 0)
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	first := []Record{{0, []byte("zero")}, {10, []byte("ten")}}
	second := []Record{{100, []byte("hundred")}}
	if err := tail.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := tail.Append(second); err != nil {
		t.Fatal(err)
	}
	before, err := tail.State()
	if err != nil {
		t.Fatal(err)
	}
	tail, err = OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	after, err := tail.State()
	if err != nil || before != after || after.Count != 3 || after.Last != 100 || !after.HasRecords {
		t.Fatal(after, err)
	}
	var records []Record
	data := tailBytes(t, f)
	state, err := ScanTail(bytes.NewReader(data), int64(len(data)), func(r Record) error { records = append(records, r); return nil })
	if err != nil || state != after || !reflect.DeepEqual(records, append(first, second...)) {
		t.Fatal(state, records, err)
	}
	var sealed bytes.Buffer
	if err := WriteSegment(&sealed, Coverage{0, 101}, sequence(records...), Options{Compression: ZstdDefault}); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, open(t, sealed.Bytes())); !reflect.DeepEqual(got, records) {
		t.Fatal("seal changed records")
	}
}

func TestTailTruncationBoundaries(t *testing.T) {
	f := tailFile(t, 5)
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := tail.Append([]Record{{5, []byte("first")}}); err != nil {
		t.Fatal(err)
	}
	first, _ := tail.State()
	if err := tail.Append([]Record{{10, []byte("second")}, {20, []byte("third")}}); err != nil {
		t.Fatal(err)
	}
	data := tailBytes(t, f)
	for n := 0; n <= len(data); n++ {
		var ids []uint64
		state, err := ScanTail(bytes.NewReader(data[:n]), int64(n), func(r Record) error { ids = append(ids, r.ID); return nil })
		switch {
		case n < tailHeaderSize:
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("short header %d: %v", n, err)
			}
		case n == tailHeaderSize || int64(n) == first.End || n == len(data):
			if err != nil {
				t.Fatalf("complete boundary %d: %v", n, err)
			}
		default:
			if !errors.Is(err, ErrIncompleteTail) {
				t.Fatalf("partial group %d: %v", n, err)
			}
		}
		if n >= tailHeaderSize && int64(n) < first.End && (state.End != tailHeaderSize || len(ids) != 0) {
			t.Fatal("delivered incomplete first group", n, state, ids)
		}
		if int64(n) >= first.End && n < len(data) && (state.End != first.End || !reflect.DeepEqual(ids, []uint64{5})) {
			t.Fatal("delivered incomplete second group", n, state, ids)
		}
	}
	// An opener never truncates, even when the scanner recognizes an incomplete suffix.
	if err := f.Truncate(int64(len(data) - 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTail(f); !errors.Is(err, ErrIncompleteTail) {
		t.Fatal(err)
	}
	if got := tailBytes(t, f); !bytes.Equal(got, data[:len(data)-1]) {
		t.Fatal("open modified incomplete tail")
	}
}

func TestTailCorruption(t *testing.T) {
	f := tailFile(t, 0)
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := tail.Append([]Record{{1, []byte("a")}, {5, []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	data := tailBytes(t, f)
	for i := range data {
		bad := bytes.Clone(data)
		bad[i] ^= 1
		state, err := ScanTail(bytes.NewReader(bad), int64(len(bad)), func(r Record) error { t.Fatalf("delivered corrupt group at byte %d", i); return nil })
		if err == nil || errors.Is(err, ErrIncompleteTail) {
			t.Fatalf("corruption interpreted as incomplete at %d: %+v %v", i, state, err)
		}
	}
	// A valid header with impossible size/count is corruption, not truncation.
	for _, offset := range []int{8, 12} {
		bad := bytes.Clone(data)
		h := bad[tailHeaderSize : tailHeaderSize+groupHeaderSize]
		format.LE.PutUint32(h[offset:], ^uint32(0))
		format.LE.PutUint32(h[28:], format.CRC(h[:28]))
		if _, err := ScanTail(bytes.NewReader(bad), int64(len(bad)), nil); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}

type faultyTail struct {
	*os.File
	syncErr, writeErr error
	short             bool
	syncs, writes     int
}

func (f *faultyTail) Sync() error {
	f.syncs++
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}
func (f *faultyTail) WriteAt(b []byte, offset int64) (int, error) {
	f.writes++
	if f.short {
		n, err := f.File.WriteAt(b[:len(b)/2], offset)
		return n, err
	}
	n, err := f.File.WriteAt(b, offset)
	if err != nil {
		return n, err
	}
	return n, f.writeErr
}

func TestTailFailurePoisonsAndRecoveryReplays(t *testing.T) {
	boom := errors.New("I/O failed")
	for _, mode := range []string{"sync", "short", "complete-write-error"} {
		t.Run(mode, func(t *testing.T) {
			f := &faultyTail{File: tailFile(t, 0)}
			tail, err := OpenTail(f)
			if err != nil {
				t.Fatal(err)
			}
			initial, _ := tail.State()
			switch mode {
			case "sync":
				f.syncErr = boom
			case "short":
				f.short = true
			default:
				f.writeErr = boom
			}
			err = tail.Append([]Record{{10, []byte("payload")}})
			if !errors.Is(err, ErrTailPoisoned) {
				t.Fatal(err)
			}
			if mode == "short" {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, boom) {
				t.Fatal(err)
			}
			state, err := tail.State()
			if state != initial || !errors.Is(err, ErrTailPoisoned) {
				t.Fatal(state, err)
			}
			writes, syncs := f.writes, f.syncs
			if err := tail.Append([]Record{{20, []byte("later")}}); !errors.Is(err, ErrTailPoisoned) {
				t.Fatal(err)
			}
			if f.writes != writes || f.syncs != syncs {
				t.Fatal("continued I/O after failure")
			}
			f.syncErr = nil
			f.writeErr = nil
			f.short = false
			recovered, err := OpenTail(f)
			if mode == "short" {
				if !errors.Is(err, ErrIncompleteTail) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			state, err = recovered.State()
			if err != nil || state.Last != 10 || state.Count != 1 {
				t.Fatal("lost complete unacknowledged group", state, err)
			}
			if err := recovered.Append([]Record{{20, []byte("later")}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTailInvalidInputAndVisitorErrors(t *testing.T) {
	f := &faultyTail{File: tailFile(t, 5)}
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, records := range [][]Record{nil, {{ID: 4}}, {{ID: ^uint64(0)}}, {{ID: 10}, {ID: 10}}, {{ID: 10}, {ID: 9}}, {{ID: 10, Payload: make([]byte, format.MaxPayload+1)}}} {
		if err := tail.Append(records); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if f.writes != 0 {
		t.Fatal("invalid input wrote bytes")
	}
	if err := tail.Append([]Record{{5, []byte("ok")}, {9, []byte("ok")}}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("visitor stopped")
	data := tailBytes(t, f.File)
	state, err := ScanTail(bytes.NewReader(data), int64(len(data)), func(r Record) error { return boom })
	if !errors.Is(err, boom) || state.End != tailHeaderSize {
		t.Fatal(state, err)
	}
	if err := tail.Append([]Record{{ID: 9}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func FuzzTail(f *testing.F) {
	file := tailFile(f, 0)
	tail, err := OpenTail(file)
	if err != nil {
		f.Fatal(err)
	}
	if err := tail.Append([]Record{{0, []byte("zero")}, {100, []byte("hundred")}}); err != nil {
		f.Fatal(err)
	}
	f.Add(tailBytes(f, file))
	var header bytes.Buffer
	if err := WriteTailHeader(&header, 0); err != nil {
		f.Fatal(err)
	}
	f.Add(header.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		var count uint64
		state, err := ScanTail(bytes.NewReader(data), int64(len(data)), func(r Record) error { count++; return nil })
		if err == nil && (state.End != int64(len(data)) || state.Count != count) {
			t.Fatal("inconsistent successful scan")
		}
	})
}

func TestTailGroupSizeBound(t *testing.T) {
	f := &faultyTail{File: tailFile(t, 0)}
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, format.MaxPayload)
	if err := tail.Append([]Record{{1, payload}, {2, payload}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversize group", err)
	}
	if f.writes != 0 {
		t.Fatal("oversize group wrote bytes")
	}
	if err := tail.Append([]Record{{1, payload}}); err != nil {
		t.Fatal("maximum record", err)
	}
	state, _ := tail.State()
	got, err := ScanTail(f, state.End, nil)
	if err != nil || got != state {
		t.Fatal(got, err)
	}
}

func TestTailOpenSyncFailure(t *testing.T) {
	boom := errors.New("sync failed")
	f := &faultyTail{File: tailFile(t, 0), syncErr: boom}
	tail, err := OpenTail(f)
	if tail != nil || !errors.Is(err, boom) || f.writes != 0 {
		t.Fatal(tail, err)
	}
}

func TestTailSemanticCorruption(t *testing.T) {
	f := tailFile(t, 0)
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := tail.Append([]Record{{1, []byte("a")}, {5, []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	data := tailBytes(t, f)
	// Forge a duplicate ID while fixing all checksums and the declared last ID.
	// Structural validation must still reject the group before delivering records.
	h := data[tailHeaderSize : tailHeaderSize+groupHeaderSize]
	format.LE.PutUint64(h[16:], 1)
	format.LE.PutUint32(h[28:], format.CRC(h[:28]))
	second := data[tailHeaderSize+groupHeaderSize+17 : len(data)-groupTrailerSize]
	format.LE.PutUint64(second[4:], 1)
	format.LE.PutUint32(second[len(second)-4:], format.CRC(second[:len(second)-4]))
	format.LE.PutUint32(data[len(data)-4:], format.CRC(data[tailHeaderSize:len(data)-4]))
	if _, err := ScanTail(bytes.NewReader(data), int64(len(data)), func(r Record) error { t.Fatal("delivered invalid ordering"); return nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
