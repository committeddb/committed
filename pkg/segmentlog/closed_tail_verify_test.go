package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestClosedTailDigestSinglePass(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteTailHeader(&buf, 0); err != nil {
		t.Fatal(err)
	}
	buf.Write(encodeTailGroup([]Record{{0, []byte("first")}, {10, nil}}, 37))
	buf.Write(encodeTailGroup([]Record{{30, bytes.Repeat([]byte("x"), 80)}}, 96))
	raw := buf.Bytes()
	ref := SegmentRef{Coverage: Coverage{0, 31}, Count: 3, TailBytes: int64(len(raw)), SHA256: sha256.Sum256(raw)}
	counted := &countedSegmentReader{data: raw, reads: make([]int, len(raw))}
	if err := verifySegmentDigest(counted, int64(len(raw)), ref); err != nil {
		t.Fatal(err)
	}
	for off, reads := range counted.reads {
		if reads != 1 {
			t.Fatalf("byte %d read %d times", off, reads)
		}
	}
	injected := errors.New("injected read failure")
	for _, off := range []int64{0, tailHeaderSize, tailHeaderSize + groupHeaderSize, int64(len(raw) - 1)} {
		counted := &countedSegmentReader{data: raw, reads: make([]int, len(raw)), failAt: off, failure: injected}
		if err := verifySegmentDigest(counted, int64(len(raw)), ref); !errors.Is(err, injected) {
			t.Fatal("lost I/O failure", off, err)
		}
	}
	// A matching whole-file hash cannot excuse corrupt framing.
	damaged := bytes.Clone(raw)
	damaged[tailHeaderSize+groupHeaderSize+12] ^= 1
	ref.SHA256 = sha256.Sum256(damaged)
	if err := verifySegmentDigest(bytes.NewReader(damaged), int64(len(damaged)), ref); !errors.Is(err, ErrCorrupt) {
		t.Fatal("accepted corrupt frame with matching file hash", err)
	}
}

func TestClosedTailDigestValidatesRecordOrder(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteTailHeader(&buf, 0); err != nil {
		t.Fatal(err)
	}
	// The encoder produces valid frame/group checksums but does not enforce order.
	buf.Write(encodeTailGroup([]Record{{10, nil}, {5, nil}}, 32))
	raw := buf.Bytes()
	ref := SegmentRef{Coverage: Coverage{0, 20}, Count: 2, TailBytes: int64(len(raw)), SHA256: sha256.Sum256(raw)}
	if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), ref); !errors.Is(err, ErrCorrupt) {
		t.Fatal("accepted unordered records with valid checksums", err)
	}
}

type verificationReadCounter struct {
	*bytes.Reader
	bytes int64
}

func (r *verificationReadCounter) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	r.bytes += int64(n)
	return n, err
}

func BenchmarkClosedTailVerification(b *testing.B) {
	const count = 5120
	var buf bytes.Buffer
	if err := WriteTailHeader(&buf, 0); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < count; start += 64 {
		records := make([]Record, 64)
		for i := range records {
			records[i] = Record{uint64(start + i), bytes.Repeat([]byte("x"), 4080)}
		}
		buf.Write(encodeTailGroup(records, 64*4096))
	}
	raw := buf.Bytes()
	ref := SegmentRef{Coverage: Coverage{0, count}, Count: count, TailBytes: int64(len(raw)), SHA256: sha256.Sum256(raw)}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	var readBytes int64
	for b.Loop() {
		reader := &verificationReadCounter{Reader: bytes.NewReader(raw)}
		if err := verifySegmentDigest(reader, int64(len(raw)), ref); err != nil {
			b.Fatal(err)
		}
		readBytes += reader.bytes
	}
	b.ReportMetric(float64(readBytes)/float64(b.N), "file-read-B/op")
}
