package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestManagedReadsRejectTailStartMismatch(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprintf("populated=%t", populated), func(t *testing.T) {
			log := newLog(t, 1024)
			if populated {
				if err := log.Append([]Record{{10, nil}}); err != nil {
					t.Fatal(err)
				}
			}
			// Keep the header checksum and all record ordering valid, but change
			// the starting ID from the catalog's 0 to 1 after startup verification.
			var header bytes.Buffer
			if err := WriteTailHeader(&header, 1); err != nil {
				t.Fatal(err)
			}
			if n, err := log.file.WriteAt(header.Bytes(), 0); err != nil || n != header.Len() {
				t.Fatal(n, err)
			}
			if r, err := log.Seek(0); !errors.Is(err, ErrCorrupt) {
				t.Fatal("seek accepted mismatched tail", r, err)
			}
			if r, err := log.Read(10); !errors.Is(err, ErrCorrupt) {
				t.Fatal("read accepted mismatched tail", r, err)
			}
			delivered := 0
			err := log.Scan(t.Context(), Coverage{0, 11}, func(Record) error { delivered++; return nil })
			if !errors.Is(err, ErrCorrupt) || delivered != 0 {
				t.Fatal("scan accepted mismatched tail", delivered, err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenLog(log.path, log.encoding)
			if reopened != nil {
				_ = reopened.Close()
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("recovery accepted mismatched tail", err)
			}
		})
	}
}

func TestManagedTailChecksStartBeforeGroups(t *testing.T) {
	var header bytes.Buffer
	if err := WriteTailHeader(&header, 1); err != nil {
		t.Fatal(err)
	}
	// The claimed group bytes are absent from this reader. Reading past the
	// header would return EOF instead of the required ownership error.
	_, err := scanManagedTail(bytes.NewReader(header.Bytes()), tailHeaderSize+groupHeaderSize, TailRef{Start: 0}, nil)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal("read group before checking catalog start", err)
	}
}
