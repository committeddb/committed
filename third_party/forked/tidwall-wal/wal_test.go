package wal

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"sync/atomic"
	"testing"
)

func dataStr(index uint64) string {
	if index%2 == 0 {
		return fmt.Sprintf("data-\"%d\"", index)
	}
	return fmt.Sprintf("data-'%d'", index)
}

func testLog(t *testing.T, opts *Options, N int) {
	logPath := "testlog/" + strings.Join(strings.Split(t.Name(), "/")[1:], "/")
	l, err := Open(logPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// FirstIndex - should be zero or one, depending on allow empty.
	n, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if !l.opts.AllowEmpty {
		if n != 0 {
			t.Fatalf("expected %d, got %d", 0, n)
		}
	} else {
		if n != 1 {
			t.Fatalf("expected %d, got %d", 1, n)
		}
	}

	// LastIndex - should be zero
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected %d, got %d", 0, n)
	}

	for i := 1; i <= N; i++ {
		// Write - try to append previous index, should fail
		err = l.Write(uint64(i-1), nil)
		if err != ErrOutOfOrder {
			t.Fatalf("expected %v, got %v", ErrOutOfOrder, err)
		}
		// Write - append next item
		err = l.Write(uint64(i), []byte(dataStr(uint64(i))))
		if err != nil {
			t.Fatalf("expected %v, got %v", nil, err)
		}
		// Write - get next item
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("expected %v, got %v", nil, err)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}

	// Read -- should fail, not found
	_, err = l.Read(0)
	if err != ErrNotFound {
		t.Fatalf("expected %v, got %v", ErrNotFound, err)
	}
	// Read -- read back all entries
	for i := 1; i <= N; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}
	// Read -- read back first half entries
	for i := 1; i <= N/2; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}
	// Read -- read second third entries
	for i := N / 3; i <= N/3+N/3; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}

	// Read -- random access
	for _, v := range rand.Perm(N) {
		index := uint64(v + 1)
		data, err := l.Read(index)
		if err != nil {
			t.Fatal(err)
		}
		if dataStr(index) != string(data) {
			t.Fatalf("expected %v, got %v", dataStr(index), string(data))
		}
	}

	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected %d, got %d", 1, n)
	}
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != uint64(N) {
		t.Fatalf("expected %d, got %d", N, n)
	}

	// Close -- close the log
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Write - try while closed
	err = l.Write(1, nil)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// WriteBatch - try while closed
	err = l.WriteBatch(nil)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// FirstIndex - try while closed
	_, err = l.FirstIndex()
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// LastIndex - try while closed
	_, err = l.LastIndex()
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// Get - try while closed
	_, err = l.Read(0)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// TruncateFront - try while closed
	err = l.TruncateFront(0)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// TruncateBack - try while closed
	err = l.TruncateBack(0)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}

	// Open -- reopen log
	l, err = Open(logPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Read -- read back all entries
	for i := 1; i <= N; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d, err=%s", i, err)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}
	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected %d, got %d", 1, n)
	}
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != uint64(N) {
		t.Fatalf("expected %d, got %d", N, n)
	}
	// Write -- add 50 more items
	for i := N + 1; i <= N+50; i++ {
		index := uint64(i)
		if err := l.Write(index, []byte(dataStr(index))); err != nil {
			t.Fatal(err)
		}
		data, err := l.Read(index)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != dataStr(index) {
			t.Fatalf("expected %v, got %v", dataStr(index), string(data))
		}
	}
	N += 50
	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected %d, got %d", 1, n)
	}
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != uint64(N) {
		t.Fatalf("expected %d, got %d", N, n)
	}
	// Batch -- test batch writes
	b := new(Batch)
	b.Write(1, nil)
	b.Write(2, nil)
	b.Write(3, nil)
	// WriteBatch -- should fail out of order
	err = l.WriteBatch(b)
	if err != ErrOutOfOrder {
		t.Fatalf("expected %v, got %v", ErrOutOfOrder, nil)
	}
	// Clear -- clear the batch
	b.Clear()
	// WriteBatch -- should succeed
	err = l.WriteBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	// Write 100 entries in batches of 10
	for i := 0; i < 10; i++ {
		for i := N + 1; i <= N+10; i++ {
			index := uint64(i)
			b.Write(index, []byte(dataStr(index)))
		}
		err = l.WriteBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		N += 10
	}
	// Read -- read back all entries
	for i := 1; i <= N; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}

	// Write -- one entry, so the buffer might be activated
	err = l.Write(uint64(N+1), []byte(dataStr(uint64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	// Read -- one random read, so there is an opened reader
	data, err := l.Read(uint64(N / 2))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != dataStr(uint64(N/2)) {
		t.Fatalf("expected %v, got %v", dataStr(uint64(N/2)), string(data))
	}

	// TruncateFront -- should fail, out of range
	for _, i := range []int{0, N + 2} {
		index := uint64(i)
		if err = l.TruncateFront(index); err != ErrOutOfRange {
			t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
		}
		testFirstLast(t, l, uint64(1), uint64(N), nil)
	}

	// TruncateBack -- should fail, out of range
	err = l.TruncateFront(0)
	if err != ErrOutOfRange {
		t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
	}
	testFirstLast(t, l, uint64(1), uint64(N), nil)

	// TruncateFront -- Remove no entries
	if err = l.TruncateFront(1); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, uint64(1), uint64(N), nil)

	// TruncateFront -- Remove first 80 entries
	if err = l.TruncateFront(81); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, uint64(81), uint64(N), nil)

	// Write -- one entry, so the buffer might be activated
	err = l.Write(uint64(N+1), []byte(dataStr(uint64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	testFirstLast(t, l, uint64(81), uint64(N), nil)

	// Read -- one random read, so there is an opened reader
	data, err = l.Read(uint64(N / 2))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != dataStr(uint64(N/2)) {
		t.Fatalf("expected %v, got %v", dataStr(uint64(N/2)), string(data))
	}

	// TruncateBack -- should fail, out of range
	for _, i := range []int{0, 79} {
		index := uint64(i)
		if err = l.TruncateBack(index); err != ErrOutOfRange {
			t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
		}
		testFirstLast(t, l, uint64(81), uint64(N), nil)
	}

	// TruncateBack -- Remove no entries
	if err = l.TruncateBack(uint64(N)); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, uint64(81), uint64(N), nil)
	// TruncateBack -- Remove last 80 entries
	if err = l.TruncateBack(uint64(N - 80)); err != nil {
		t.Fatal(err)
	}
	N -= 80
	testFirstLast(t, l, uint64(81), uint64(N), nil)

	// Read -- read back all entries
	for i := 81; i <= N; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}

	// Close -- close log after truncating
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}

	// Open -- open log after truncating
	l, err = Open(logPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	testFirstLast(t, l, uint64(81), uint64(N), nil)

	// Read -- read back all entries
	for i := 81; i <= N; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(uint64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(uint64(i)), data)
		}
	}

	// TruncateFront -- truncate all entries but one
	if err = l.TruncateFront(uint64(N)); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, uint64(N), uint64(N), nil)

	// Write -- write on entry
	err = l.Write(uint64(N+1), []byte(dataStr(uint64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	testFirstLast(t, l, uint64(N-1), uint64(N), nil)

	// TruncateBack -- truncate all entries but one
	if err = l.TruncateBack(uint64(N - 1)); err != nil {
		t.Fatal(err)
	}
	N--
	testFirstLast(t, l, uint64(N), uint64(N), nil)

	if err = l.Write(uint64(N+1), []byte(dataStr(uint64(N+1)))); err != nil {
		t.Fatal(err)
	}
	N++

	l.Sync()
	testFirstLast(t, l, uint64(N-1), uint64(N), nil)

	allowEmpty := opts != nil && opts.AllowEmpty
	// TruncateFront -- truncate all entries

	err = l.TruncateFront(uint64(N + 1))
	if allowEmpty {
		if err != nil {
			t.Fatal(err)
		}
		testFirstLast(t, l, uint64(N+1), uint64(N), nil)
	} else {
		if err != ErrOutOfRange {
			t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
		}
	}

	err = l.Write(uint64(N+1), []byte(dataStr(uint64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	if allowEmpty {
		testFirstLast(t, l, uint64(N), uint64(N), nil)
	} else {
		testFirstLast(t, l, uint64(N-2), uint64(N), nil)
	}

	// TruncateBack -- truncate all entries
	fidx, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if allowEmpty {
		if fidx != uint64(N) {
			t.Fatalf("expected %v, got %v", N, fidx)
		}
	} else {
		if fidx != uint64(N-2) {
			t.Fatalf("expected %v, got %v", N-2, fidx)
		}
	}

	err = l.TruncateBack(uint64(fidx - 1))
	if allowEmpty {
		if err != nil {
			t.Fatal(err)
		}
		testFirstLast(t, l, uint64(N), uint64(N-1), nil)
	} else {
		if err != ErrOutOfRange {
			t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
		}
	}
}

func testFirstLast(t *testing.T, l *Log, expectFirst, expectLast uint64,
	data func(index uint64) []byte,
) {
	t.Helper()
	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	li, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if fi != expectFirst || li != expectLast {
		t.Fatalf("expected %v/%v, got %v/%v", expectFirst, expectLast, fi, li)
	}
	is, err := l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if expectFirst > expectLast {
		if !is {
			t.Fatalf("expected true")
		}
	} else {
		if is {
			t.Fatalf("expected false")
		}
	}
	for i := fi; i <= li; i++ {
		dt1, err := l.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		if data != nil {
			dt2 := data(i)
			if string(dt1) != string(dt2) {
				t.Fatalf("mismatch '%s' != '%s'", dt2, dt1)
			}
		}
	}

}

func TestLog(t *testing.T) {
	os.RemoveAll("testlog")
	defer os.RemoveAll("testlog")

	t.Run("nil-opts", func(t *testing.T) {
		testLog(t, nil, 500)
	})
	t.Run("allow-empty", func(t *testing.T) {
		testLog(t, makeOpts(512, true, JSON, true), 500)
	})
	t.Run("no-sync", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			t.Run("no-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, true, JSON, false), 500)
			})
			t.Run("allow-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, true, JSON, true), 500)
			})
		})
		t.Run("binary", func(t *testing.T) {
			t.Run("no-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, true, Binary, false), 500)
			})
			t.Run("allow-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, true, Binary, true), 500)
			})
		})
	})
	t.Run("sync", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			t.Run("no-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, false, JSON, false), 100)
			})
			t.Run("allow-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, false, JSON, true), 100)
			})
		})
		t.Run("binary", func(t *testing.T) {
			t.Run("no-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, false, Binary, false), 100)
			})
			t.Run("allow-empty", func(t *testing.T) {
				testLog(t, makeOpts(512, false, Binary, true), 100)
			})
		})
	})
}

func TestOutliers(t *testing.T) {
	// Create some scenarios where the log has been corrupted, operations
	// fail, or various weirdnesses.
	t.Run("fail-in-memory", func(t *testing.T) {
		if l, err := Open(":memory:", nil); err == nil {
			l.Close()
			t.Fatal("expected error")
		}
	})
	t.Run("fail-not-a-directory", func(t *testing.T) {
		defer os.RemoveAll("testlog/file")
		if err := os.MkdirAll("testlog", 0777); err != nil {
			t.Fatal(err)
		} else if f, err := os.Create("testlog/file"); err != nil {
			t.Fatal(err)
		} else if err := f.Close(); err != nil {
			t.Fatal(err)
		} else if l, err := Open("testlog/file", nil); err == nil {
			l.Close()
			t.Fatal("expected error")
		}
	})
	t.Run("load-with-junk-files", func(t *testing.T) {
		// junk should be ignored
		defer os.RemoveAll("testlog/junk")
		if err := os.MkdirAll("testlog/junk/other1", 0777); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create("testlog/junk/other2")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		f, err = os.Create("testlog/junk/" + strings.Repeat("A", 20))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		l, err := Open("testlog/junk", nil)
		if err != nil {
			t.Fatal(err)
		}
		l.Close()
	})

	t.Run("fail-corrupted-tail-json", func(t *testing.T) {
		defer os.RemoveAll("testlog/corrupt-tail")
		opts := makeOpts(512, true, JSON, false)
		os.MkdirAll("testlog/corrupt-tail", 0777)
		ioutil.WriteFile(
			"testlog/corrupt-tail/00000000000000000001",
			[]byte("\n"), 0666)
		if l, err := Open("testlog/corrupt-tail", opts); err != ErrCorrupt {
			l.Close()
			t.Fatalf("expected %v, got %v", ErrCorrupt, err)
		}
		ioutil.WriteFile(
			"testlog/corrupt-tail/00000000000000000001",
			[]byte(`{}`+"\n"), 0666)
		if l, err := Open("testlog/corrupt-tail", opts); err != ErrCorrupt {
			l.Close()
			t.Fatalf("expected %v, got %v", ErrCorrupt, err)
		}
		ioutil.WriteFile(
			"testlog/corrupt-tail/00000000000000000001",
			[]byte(`{"index":"1"}`+"\n"), 0666)
		if l, err := Open("testlog/corrupt-tail", opts); err != ErrCorrupt {
			l.Close()
			t.Fatalf("expected %v, got %v", ErrCorrupt, err)
		}
		ioutil.WriteFile(
			"testlog/corrupt-tail/00000000000000000001",
			[]byte(`{"index":"1","data":"?"}`), 0666)
		if l, err := Open("testlog/corrupt-tail", opts); err != ErrCorrupt {
			l.Close()
			t.Fatalf("expected %v, got %v", ErrCorrupt, err)
		}
	})

	t.Run("start-marker-file", func(t *testing.T) {
		lpath := "testlog/start-marker"
		opts := makeOpts(512, true, JSON, false)
		l := must(Open(lpath, opts)).(*Log)
		defer l.Close()
		for i := uint64(1); i <= 100; i++ {
			must(nil, l.Write(i, []byte(dataStr(i))))
		}
		path := l.segments[l.findSegment(35)].path
		firstIndex := l.segments[l.findSegment(35)].index
		must(nil, l.Close())
		data := must(ioutil.ReadFile(path)).([]byte)
		must(nil, ioutil.WriteFile(path+".START", data, 0666))
		l = must(Open(lpath, opts)).(*Log)
		defer l.Close()
		testFirstLast(t, l, firstIndex, 100, nil)
	})
}

func makeOpts(segSize int, noSync bool, lf LogFormat, allowEmpty bool,
) *Options {
	opts := *DefaultOptions
	opts.SegmentSize = segSize
	opts.NoSync = noSync
	opts.LogFormat = lf
	opts.AllowEmpty = allowEmpty
	return &opts
}

// https://github.com/tidwall/wal/issues/1
func TestIssue1(t *testing.T) {
	in := []byte{0, 0, 0, 0, 0, 0, 0, 1, 37, 108, 131, 178, 151, 17, 77, 32,
		27, 48, 23, 159, 63, 14, 240, 202, 206, 151, 131, 98, 45, 165, 151, 67,
		38, 180, 54, 23, 138, 238, 246, 16, 0, 0, 0, 0}
	opts := *DefaultOptions
	opts.LogFormat = JSON
	os.RemoveAll("testlog")
	l, err := Open("testlog", &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Write(1, in); err != nil {
		t.Fatal(err)
	}
	out, err := l.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		t.Fatal("data mismatch")
	}
}

func TestSimpleTruncateFront(t *testing.T) {
	os.RemoveAll("testlog")

	opts := &Options{
		NoSync:      true,
		LogFormat:   JSON,
		SegmentSize: 100,
	}

	l, err := Open("testlog", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		l.Close()
	}()

	makeData := func(index uint64) []byte {
		return []byte(fmt.Sprintf("data-%d", index))
	}

	valid := func(t *testing.T, first, last uint64) {
		t.Helper()
		index, err := l.FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != first {
			t.Fatalf("expected %v, got %v", first, index)
		}
		index, err = l.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != last {
			t.Fatalf("expected %v, got %v", last, index)
		}
		for i := first; i <= last; i++ {
			data, err := l.Read(i)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(makeData(i)) {
				t.Fatalf("expcted '%s', got '%s'", makeData(i), data)
			}
		}
	}
	validReopen := func(t *testing.T, first, last uint64) {
		t.Helper()
		valid(t, first, last)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l, err = Open("testlog", opts)
		if err != nil {
			t.Fatal(err)
		}
		valid(t, first, last)
	}
	for i := 1; i <= 100; i++ {
		err := l.Write(uint64(i), makeData(uint64(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	validReopen(t, 1, 100)

	if err := l.TruncateFront(1); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 100)

	if err := l.TruncateFront(2); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 2, 100)

	if err := l.TruncateFront(4); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 4, 100)

	if err := l.TruncateFront(5); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 5, 100)

	if err := l.TruncateFront(99); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 99, 100)

	if err := l.TruncateFront(100); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 100, 100)

}

func TestSimpleTruncateBack(t *testing.T) {
	os.RemoveAll("testlog")

	opts := &Options{
		NoSync:      true,
		LogFormat:   JSON,
		SegmentSize: 100,
	}

	l, err := Open("testlog", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		l.Close()
	}()

	makeData := func(index uint64) []byte {
		return []byte(fmt.Sprintf("data-%d", index))
	}

	valid := func(t *testing.T, first, last uint64) {
		t.Helper()
		index, err := l.FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != first {
			t.Fatalf("expected %v, got %v", first, index)
		}
		index, err = l.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != last {
			t.Fatalf("expected %v, got %v", last, index)
		}
		for i := first; i <= last; i++ {
			data, err := l.Read(i)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(makeData(i)) {
				t.Fatalf("expcted '%s', got '%s'", makeData(i), data)
			}
		}
	}
	validReopen := func(t *testing.T, first, last uint64) {
		t.Helper()
		valid(t, first, last)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l, err = Open("testlog", opts)
		if err != nil {
			t.Fatal(err)
		}
		valid(t, first, last)
	}
	for i := 1; i <= 100; i++ {
		err := l.Write(uint64(i), makeData(uint64(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	validReopen(t, 1, 100)

	/////////////////////////////////////////////////////////////
	if err := l.TruncateBack(100); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 100)
	if err := l.Write(101, makeData(101)); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 101)

	/////////////////////////////////////////////////////////////
	if err := l.TruncateBack(99); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 99)
	if err := l.Write(100, makeData(100)); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 100)

	if err := l.TruncateBack(94); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 94)

	if err := l.TruncateBack(93); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 93)

	if err := l.TruncateBack(92); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 92)

}

func TestConcurrency(t *testing.T) {
	os.RemoveAll("testlog")

	l, err := Open("testlog", &Options{
		NoSync: true,
		NoCopy: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Write 1000 entries
	for i := 1; i <= 1000; i++ {
		err := l.Write(uint64(i), []byte(dataStr(uint64(i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Perform 100,000 reads (over 100 threads)
	finished := int32(0)
	maxIndex := int32(1000)
	numReads := int32(0)
	for i := 0; i < 100; i++ {
		go func() {
			defer atomic.AddInt32(&finished, 1)

			for i := 0; i < 1_000; i++ {
				index := rand.Int31n(atomic.LoadInt32(&maxIndex)) + 1
				if _, err := l.Read(uint64(index)); err != nil {
					panic(err)
				}
				atomic.AddInt32(&numReads, 1)
			}
		}()
	}

	// continue writing
	for index := maxIndex + 1; atomic.LoadInt32(&finished) < 100; index++ {
		err := l.Write(uint64(index), []byte(dataStr(uint64(index))))
		if err != nil {
			t.Fatal(err)
		}
		atomic.StoreInt32(&maxIndex, index)
	}

	// confirm total reads
	if exp := int32(100_000); numReads != exp {
		t.Fatalf("expected %d reads, but god %d", exp, numReads)
	}
}

func TestRWConcurrency(t *testing.T) {
	os.RemoveAll("testlog")

	l, err := Open("testlog", &Options{
		NoSync:     true,
		NoCopy:     true,
		AllowEmpty: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)
	notify := make(chan struct{}, 1)
	count := 100

	go func() {
		defer wg.Done()
		defer close(notify)
		idx, _ := l.LastIndex()
		for i := 0; i < count; i++ {
			idx++
			if err := l.Write(idx, []byte(dataStr(uint64(i)))); err != nil {
				panic(err)
			}
			select {
			case notify <- struct{}{}:
			default:
			}
		}
		if idx != uint64(count) {
			panic(fmt.Sprintf("expected last index %d, got %d", count, idx))
		}
	}()

	go func() {
		defer wg.Done()
		idx, _ := l.FirstIndex()
		for range notify {
			for {
				_, err := l.Read(idx)
				if errors.Is(err, ErrNotFound) {
					break
				}
				if err := l.TruncateFront(idx + 1); err != nil {
					panic(err)
				}
				idx++
			}
		}
		if idx != uint64(count+1) {
			panic(fmt.Sprintf("expected first index %d, got %d", count+1, idx))
		}
	}()

	wg.Wait()
}

func must(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}

func TestNoAllowEmpty(t *testing.T) {
	os.RemoveAll("testlog")
	l, err := Open("testlog", &Options{
		NoSync:      true,
		NoCopy:      true,
		AllowEmpty:  false,
		SegmentSize: 4096,
		LogFormat:   JSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if l != nil {
			l.Close()
		}
	}()
	firstIndex, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	lastIndex, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != 0 || lastIndex != 0 {
		t.Fatalf("expected %d %d, got %d %d\n", 0, 0, firstIndex, lastIndex)
	}
	N := 1000
	for i := 0; i < N; i++ {
		err := l.Write(uint64(i)+1, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = l.TruncateFront(uint64(N + 1))
	if err != ErrOutOfRange {
		t.Fatalf("expected %v, got %v\n", ErrOutOfRange, err)
	}
	err = l.TruncateBack(0)
	if err != ErrOutOfRange {
		t.Fatalf("expected %v, got %v\n", ErrOutOfRange, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen, allowing empty
	l, err = Open("testlog", &Options{
		NoSync:      true,
		NoCopy:      true,
		AllowEmpty:  true,
		SegmentSize: 4096,
		LogFormat:   JSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = l.TruncateFront(uint64(N + 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen, not allowing empty
	l, err = Open("testlog", &Options{
		NoSync:      true,
		NoCopy:      true,
		AllowEmpty:  false,
		SegmentSize: 4096,
		LogFormat:   JSON,
	})
	if err != ErrEmptyLog {
		t.Fatalf("expected %v, got %v", ErrEmptyLog, err)
	}
}

func TestEmptyOpenFromNothing(t *testing.T) {
	os.RemoveAll("testlog")
	l, err := Open("testlog", &Options{
		NoSync:     true,
		NoCopy:     true,
		AllowEmpty: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	isEmpty, err := l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if isEmpty == false {
		t.Fatal("expected true")
	}
	firstIndex, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	lastIndex, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != 1 || lastIndex != 0 {
		t.Fatalf("expected %d %d, got %d %d\n", 1, 0, firstIndex, lastIndex)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyOpenFromExisting1(t *testing.T) {
	os.RemoveAll("testlog")
	os.Mkdir("testlog", 0777)
	os.WriteFile("testlog/00000000000000000001", nil, 0666)
	l, err := Open("testlog", &Options{
		NoSync:     true,
		NoCopy:     true,
		AllowEmpty: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	isEmpty, err := l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if isEmpty == false {
		t.Fatal("expected true")
	}
	firstIndex, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	lastIndex, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != 1 || lastIndex != 0 {
		t.Fatalf("expected %d %d, got %d %d\n", 1, 0, firstIndex, lastIndex)
	}
}

func TestEmptyOpenFromExisting1001(t *testing.T) {
	os.RemoveAll("testlog")
	os.Mkdir("testlog", 0777)
	os.WriteFile("testlog/00000000000000001001", nil, 0666)
	l, err := Open("testlog", &Options{
		NoSync:     true,
		NoCopy:     true,
		AllowEmpty: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	isEmpty, err := l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if isEmpty == false {
		t.Fatal("expected true")
	}
	firstIndex, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	lastIndex, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != 1001 || lastIndex != 1000 {
		t.Fatalf("expected %d %d, got %d %d\n", 0, 0, firstIndex, lastIndex)
	}
}

func TestEmptyTruncateFrontTwice(t *testing.T) {
	os.RemoveAll("testlog")
	l, err := Open("testlog", &Options{
		NoSync:      true,
		NoCopy:      true,
		AllowEmpty:  true,
		SegmentSize: 4096,
		LogFormat:   JSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	N := 1000
	for i := 0; i < N; i++ {
		err := l.Write(uint64(i)+1, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = l.TruncateFront(uint64(N) + 1)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if !empty {
		t.Fatalf("expected %v, got %v", true, empty)
	}
	index, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if index != uint64(N+1) {
		t.Fatalf("expected %v, got %v", uint64(N+1), index)
	}
	err = l.TruncateFront(uint64(N) + 1)
	if err != nil {
		t.Fatal(err)
	}
	empty, err = l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if !empty {
		t.Fatalf("expected %v, got %v", true, empty)
	}
	index, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if index != uint64(N+1) {
		t.Fatalf("expected %v, got %v", uint64(N+1), index)
	}
}

func TestEmptyTruncateBackTwice(t *testing.T) {
	os.RemoveAll("testlog")
	l, err := Open("testlog", &Options{
		NoSync:      true,
		NoCopy:      true,
		AllowEmpty:  true,
		SegmentSize: 4096,
		LogFormat:   JSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	N := 1000
	for i := 0; i < N; i++ {
		err := l.Write(uint64(i)+1, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = l.TruncateBack(0)
	if err != nil {
		t.Fatal(err)
	}
	fidx, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	lidx, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if fidx != 1 || lidx != 0 {
		t.Fatalf("expected %v/%v, got %v/%v", 1, 0, fidx, lidx)
	}
	empty, err := l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if !empty {
		t.Fatalf("expected %v, got %v", true, empty)
	}
	err = l.TruncateBack(0)
	if err != nil {
		t.Fatal(err)
	}
	fidx, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	lidx, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if fidx != 1 || lidx != 0 {
		t.Fatalf("expected %v/%v, got %v/%v", 1, 0, fidx, lidx)
	}
	empty, err = l.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if !empty {
		t.Fatalf("expected %v, got %v", true, empty)
	}
}

func TestIssue33(t *testing.T) {
	// Create a fresh log without allowempty, close, and reopen.
	// This should not fail with an ErrEmptyLog
	os.RemoveAll("testlog")
	l, err := Open("log", nil)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	l, err = Open("log", nil)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}

// TestMisTiledSegmentReadErrors is a committeddb fork regression test: a
// non-tail segment file whose content does not tile exactly to the next
// segment's start index must make reads into it return ErrCorrupt — never
// panic — while reads of healthy segments and appends keep working. The
// binary format stores no per-entry index (identity = filename start +
// position), Open validates only the tail, and before the fork's tiling
// check an empty or entry-boundary-truncated middle segment parsed
// "successfully" short and Read's epos indexing panicked the process on the
// first cold historical read (the post-scrub crashloop). Three shapes:
// hollow (0 bytes), short (truncated at an entry boundary), and long
// (trailing zero bytes parse as extra size-0 entries, which would misindex
// every later entry in the segment).
func TestMisTiledSegmentReadErrors(t *testing.T) {
	// Fixed-size entries make segment geometry deterministic: data is 10
	// bytes, so each binary entry is 1 (uvarint len) + 10 = 11 bytes, and
	// SegmentSize 110 cycles after exactly 10 entries per segment.
	makeData := func(index uint64) []byte {
		return []byte(fmt.Sprintf("d%09d", index))
	}
	const entrySize = 11
	const perSegment = 10

	build := func(t *testing.T) (string, *Options) {
		t.Helper()
		dir := t.TempDir()
		opts := &Options{NoSync: true, SegmentSize: entrySize * perSegment}
		l, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		// 35 entries -> segments starting at 1, 11, 21, 31 (tail).
		for i := uint64(1); i <= 35; i++ {
			if err := l.Write(i, makeData(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		return dir, opts
	}
	segPath := func(dir string, start uint64) string {
		return filepath.Join(dir, segmentName(start))
	}

	corruptions := []struct {
		name    string
		corrupt func(t *testing.T, path string)
	}{
		{"hollow (0 bytes)", func(t *testing.T, path string) {
			t.Helper()
			if err := os.Truncate(path, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{"short (truncated at an entry boundary)", func(t *testing.T, path string) {
			t.Helper()
			if err := os.Truncate(path, entrySize*5); err != nil {
				t.Fatal(err)
			}
		}},
		{"long (trailing zero bytes)", func(t *testing.T, path string) {
			t.Helper()
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(make([]byte, entrySize)); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			dir, opts := build(t)
			tc.corrupt(t, segPath(dir, 21)) // middle segment: entries 21-30

			// Open still succeeds: only the tail is parsed at load.
			l, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()

			// Every read into the mis-tiled segment errors; none panic.
			for i := uint64(21); i <= 30; i++ {
				if _, err := l.Read(i); err != ErrCorrupt {
					t.Fatalf("Read(%d) = %v, want ErrCorrupt", i, err)
				}
			}
			// Healthy segments, before and after the hole, still read fine.
			for _, i := range []uint64{1, 10, 11, 20, 31, 35} {
				data, err := l.Read(i)
				if err != nil {
					t.Fatalf("Read(%d) healthy segment: %v", i, err)
				}
				if string(data) != string(makeData(i)) {
					t.Fatalf("Read(%d) = %q, want %q", i, data, makeData(i))
				}
			}
			// Appends keep working past the hole (a hole in history must not
			// stop the tail of the log), and the new entry reads back.
			if err := l.Write(36, makeData(36)); err != nil {
				t.Fatal(err)
			}
			data, err := l.Read(36)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(makeData(36)) {
				t.Fatalf("Read(36) = %q, want %q", data, makeData(36))
			}
		})
	}
}

// TestConcurrentReadEvictionRace is a committeddb fork regression test for the
// concurrent-read data race that crashed a production node: Read runs under
// mu.RLock (shared — many readers at once, as the docs advertise), but
// loadSegment→pushCache MUTATES shared state on that path — lazily loading a
// segment's entry table and, on cache eviction, nil-ing ANOTHER segment's
// table — with no synchronization between readers. Reader A, between its
// "table loaded?" check and its epos[index-s.index] use, loses its table to
// reader B's eviction and panics with "index out of range [k] with length 0".
// Eight readers pinned to distinct segments against a 1-slot cache make the
// collision near-certain within milliseconds. Before the per-segment lock fix
// this test fails under -race (and can panic outright); after, it is clean and
// every read returns correct data — a reader that loses the eviction race must
// reload, never error and never panic.
func TestConcurrentReadEvictionRace(t *testing.T) {
	makeData := func(index uint64) []byte {
		return []byte(fmt.Sprintf("d%09d", index))
	}
	const entrySize = 11
	const perSegment = 10

	dir := t.TempDir()
	opts := &Options{NoSync: true, SegmentSize: entrySize * perSegment, SegmentCacheSize: 1}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	// 85 entries -> segments starting at 1,11,...,71 plus the tail at 81.
	for i := uint64(1); i <= 85; i++ {
		if err := l.Write(i, makeData(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Reopen so only the tail is resident: every non-tail read goes through
	// the lazy-load + eviction machinery.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Eight readers, each pinned to its own non-tail segment, reading in a
	// loop: every load by one evicts another's segment from the 1-slot cache.
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	stop := make(chan struct{})
	for g := 0; g < 8; g++ {
		base := uint64(1 + g*perSegment)
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := base; i < base+perSegment; i++ {
					data, err := l.Read(i)
					if err != nil {
						errCh <- fmt.Errorf("Read(%d): %w", i, err)
						return
					}
					if string(data) != string(makeData(i)) {
						errCh <- fmt.Errorf("Read(%d) returned wrong data", i)
						return
					}
				}
			}
		}(base)
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

// TestSegmentCacheEvictsByLoadOrder is a committeddb fork regression test
// pinning two properties of the read path's cache discipline:
//
//  1. Reading an already-loaded segment takes the lock-free-ish fast path —
//     no re-parse, and crucially NO cache push. The original flow pushed to
//     the cache (an exclusive lock) on every read, which serialized N
//     concurrent replaying readers down to ~1× single-reader throughput.
//  2. Consequently cache recency is LRU-by-LOAD, not LRU-by-access: a
//     segment read many times since load is still evicted before a segment
//     loaded after it. This is the deliberate trade-off that removes the
//     per-entry serialization; an innocent "fix" restoring push-on-read
//     recency would reintroduce the serializer and must fail here.
func TestSegmentCacheEvictsByLoadOrder(t *testing.T) {
	makeData := func(index uint64) []byte {
		return []byte(fmt.Sprintf("d%09d", index))
	}
	const entrySize = 11
	const perSegment = 10

	dir := t.TempDir()
	opts := &Options{NoSync: true, SegmentSize: entrySize * perSegment, SegmentCacheSize: 2}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	// 35 entries -> segments starting at 1, 11, 21, 31 (tail).
	for i := uint64(1); i <= 35; i++ {
		if err := l.Write(i, makeData(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen so only the tail is parsed and the cache starts empty (cycle()
	// pushes rotated-out segments during the writes above, which would make
	// the eviction accounting depend on write history rather than reads).
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	mustRead := func(index uint64) {
		t.Helper()
		data, err := l.Read(index)
		if err != nil {
			t.Fatalf("Read(%d): %v", index, err)
		}
		if string(data) != string(makeData(index)) {
			t.Fatalf("Read(%d) = %q, want %q", index, data, makeData(index))
		}
	}

	base := l.SegmentLoads()
	mustRead(1)  // populate segment 1        -> loads +1, cache [1]
	mustRead(11) // populate segment 11       -> loads +2, cache [11, 1]
	if got := l.SegmentLoads() - base; got != 2 {
		t.Fatalf("after populating two segments: %d loads, want 2", got)
	}

	// Property 1: repeated reads of loaded segments re-parse nothing.
	for i := 0; i < 50; i++ {
		mustRead(1)
		mustRead(2)
	}
	if got := l.SegmentLoads() - base; got != 2 {
		t.Fatalf("loaded-segment reads re-parsed: %d loads, want 2", got)
	}

	// Property 2: loading segment 21 evicts segment 1 — the LEAST RECENTLY
	// LOADED — even though it was read 100 times after segment 11 was.
	// (Push-on-read recency would evict segment 11 instead.)
	mustRead(21) // populate segment 21 -> loads +3, evicts segment 1
	mustRead(11) // must still be cached under load-order recency
	if got := l.SegmentLoads() - base; got != 3 {
		t.Fatalf("segment 11 was evicted (access-order recency?): %d loads, want 3", got)
	}
	mustRead(1) // was evicted -> must re-parse
	if got := l.SegmentLoads() - base; got != 4 {
		t.Fatalf("segment 1 read after eviction: %d loads, want 4", got)
	}
}
