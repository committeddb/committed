package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

type countedSegmentReader struct {
	data    []byte
	reads   []int
	failAt  int64
	failure error
}

func (r *countedSegmentReader) ReadAt(p []byte, off int64) (int, error) {
	if r.failure != nil && off <= r.failAt && off+int64(len(p)) > r.failAt {
		return 0, r.failure
	}
	n, err := bytes.NewReader(r.data).ReadAt(p, off)
	for i := range n {
		r.reads[int(off)+i]++
	}
	return n, err
}

func digestFixture(t *testing.T, codec Compression) []byte {
	t.Helper()
	var buf bytes.Buffer
	records := []Record{{ID: 0, Payload: bytes.Repeat([]byte("a"), 80)}, {ID: 10, Payload: []byte("second")}, {ID: 30, Payload: bytes.Repeat([]byte("b"), 80)}}
	if err := WriteSegment(&buf, Coverage{0, 100}, sequence(records...), Options{BlockSize: 64, Compression: codec}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func digestRef(t *testing.T, raw []byte) SegmentRef {
	t.Helper()
	segment := open(t, raw)
	return SegmentRef{Coverage: segment.Coverage(), Count: segment.Count(), SHA256: sha256.Sum256(raw)}
}

func TestSegmentDigestSinglePayloadPass(t *testing.T) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		raw := digestFixture(t, codec)
		ref := digestRef(t, raw)
		counted := &countedSegmentReader{data: raw, reads: make([]int, len(raw))}
		if err := verifySegmentDigest(counted, int64(len(raw)), ref); err != nil {
			t.Fatal(err)
		}
		segment := open(t, raw)
		for _, block := range segment.blocks {
			for off := block.offset; off < block.offset+uint64(block.size); off++ {
				if counted.reads[off] != 1 {
					t.Fatalf("codec %d: payload offset %d read %d times", codec, off, counted.reads[off])
				}
			}
		}
		for off, n := range counted.reads {
			if n == 0 {
				t.Fatalf("unverified file byte %d", off)
			}
		}
		wrong := ref
		wrong.SHA256[0] ^= 1
		if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), wrong); !errors.Is(err, ErrCorrupt) {
			t.Fatal("accepted wrong digest", err)
		}
		wrong = ref
		wrong.Count++
		if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), wrong); !errors.Is(err, ErrCorrupt) {
			t.Fatal("accepted wrong count", err)
		}
		wrong = ref
		wrong.Coverage.End++
		if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), wrong); !errors.Is(err, ErrCorrupt) {
			t.Fatal("accepted wrong coverage", err)
		}
		injected := errors.New("injected payload read failure")
		counted = &countedSegmentReader{data: raw, reads: make([]int, len(raw)), failAt: int64(segment.blocks[0].offset), failure: injected}
		if err := verifySegmentDigest(counted, int64(len(raw)), ref); !errors.Is(err, injected) {
			t.Fatal("lost read failure", err)
		}
	}
}

func TestSegmentDigestRetainsSemanticVerification(t *testing.T) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		raw := digestFixture(t, codec)
		ref := digestRef(t, raw)
		for i := range raw {
			corrupt := bytes.Clone(raw)
			corrupt[i] ^= 1
			// A matching external SHA must not bypass internal format/frame checks.
			ref.SHA256 = sha256.Sum256(corrupt)
			if err := verifySegmentDigest(bytes.NewReader(corrupt), int64(len(corrupt)), ref); err == nil {
				t.Fatalf("codec %d: accepted corrupt byte %d with matching digest", codec, i)
			}
		}
		for size := range len(raw) {
			ref.SHA256 = sha256.Sum256(raw[:size])
			if err := verifySegmentDigest(bytes.NewReader(raw[:size]), int64(size), ref); err == nil {
				t.Fatalf("accepted truncation at %d", size)
			}
		}
	}
}

func TestSegmentDigestEmptyAndLegacy(t *testing.T) {
	var empty bytes.Buffer
	if err := WriteSegment(&empty, Coverage{0, 100}, sequence(), Options{}); err != nil {
		t.Fatal(err)
	}
	raw := empty.Bytes()
	if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), digestRef(t, raw)); err != nil {
		t.Fatal(err)
	}
	// The same format-0 fixture used by the public reader's compatibility test.
	legacy, err := hex.DecodeString("5345474c4f473030000000000000000000000000e803000000000000349a76750300000007000000000000006f6c64392d228e0700000000000000070000000000000020000000000000001300000001000000c74b674800000000330000000000000001000000010000000000000033e6ea6e454e4430ed8597bd")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifySegmentDigest(bytes.NewReader(legacy), int64(len(legacy)), digestRef(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// A truncated backing reader must return its I/O error even when declared size
	// and the reference still describe the complete file.
	if err := verifySegmentDigest(bytes.NewReader(raw[:len(raw)-1]), int64(len(raw)), digestRef(t, raw)); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestSegmentDigestChecksFramesWithValidOuterChecksums(t *testing.T) {
	raw := digestFixture(t, NoCompression)
	ref := digestRef(t, raw)
	segment := open(t, raw)
	first := segment.blocks[0]
	raw[first.offset+12] ^= 1 // Alter payload while leaving its frame CRC invalid.
	footer := raw[len(raw)-format.FooterSize:]
	indexOffset := format.LE.Uint64(footer)
	index := raw[indexOffset:uint64(len(raw)-format.FooterSize)]
	format.LE.PutUint32(index[32:], format.CRC(raw[first.offset:first.offset+uint64(first.size)]))
	format.LE.PutUint32(footer[20:], format.CRC(index))
	format.LE.PutUint32(footer[28:], format.CRC(footer[:28]))
	ref.SHA256 = sha256.Sum256(raw)
	if err := verifySegmentDigest(bytes.NewReader(raw), int64(len(raw)), ref); !errors.Is(err, ErrCorrupt) {
		t.Fatal("accepted corrupt frame with valid enclosing checksums", err)
	}
}
