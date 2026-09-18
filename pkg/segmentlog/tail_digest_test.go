package segmentlog

import (
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

type countedTailReads struct {
	TailFile
	bytes int
}

func (f *countedTailReads) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.TailFile.ReadAt(p, off)
	f.bytes += n
	return n, err
}

func checkTailDigest(t *testing.T, tail *Tail) {
	t.Helper()
	state, digest, err := tail.rolloverState()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.NewSectionReader(tail.file, 0, state.End)); err != nil {
		t.Fatal(err)
	}
	var want [sha256.Size]byte
	hash.Sum(want[:0])
	if digest != want {
		t.Fatal("incremental digest differs from physical bytes")
	}
}

func TestTailDigestAppendAndRecovery(t *testing.T) {
	file := &countedTailReads{TailFile: tailFile(t, 5)}
	tail, err := OpenTail(file)
	if err != nil {
		t.Fatal(err)
	}
	checkTailDigest(t, tail)
	for _, records := range [][]Record{{{5, nil}, {12, []byte("first")}}, {{90, []byte("second group")}}} {
		if err := tail.Append(records); err != nil {
			t.Fatal(err)
		}
		checkTailDigest(t, tail)
		file.bytes = 0
		tail, err = OpenTail(file)
		if err != nil {
			t.Fatal(err)
		}
		state, _, err := tail.rolloverState()
		if err != nil || int64(file.bytes) != state.End {
			t.Fatal("recovery must hash in its existing scan", file.bytes, state.End, err)
		}
		checkTailDigest(t, tail)
	}
	if err := tail.Append([]Record{{90, nil}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	checkTailDigest(t, tail)
}

func TestManagedTailDigestAfterRewrite(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "erased"
		if partial {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			log := newLog(t, 64)
			records := []Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}, {40, nil}}
			if partial {
				records = append(records, Record{50, []byte("survivor")})
			}
			if err := log.Append(records); err != nil {
				t.Fatal(err)
			}
			// The active file was installed with its first group during rollover.
			checkTailDigest(t, log.tail)
			if _, err := log.Rewrite(t.Context(), 1, eraseEvenTens); err != nil {
				t.Fatal(err)
			}
			checkTailDigest(t, log.tail)
			log = reopenLog(t, log)
			checkTailDigest(t, log.tail)
			if err := log.Append([]Record{{60, nil}, {70, nil}, {80, nil}, {90, nil}}); err != nil {
				t.Fatal(err)
			}
			checkTailDigest(t, log.tail)
			if err := log.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
