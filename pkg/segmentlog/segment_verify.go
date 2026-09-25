package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"io"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// verifySegmentDigest verifies file contents and metadata against the reference.
// The ReaderAt must remain immutable. Indexed payload blocks are read once;
// closed append files combine record validation and hashing in one pass.
func verifySegmentDigest(r io.ReaderAt, size int64, ref SegmentRef) error {
	if ref.TailBytes != 0 {
		digest := sha256.New()
		if err := checkClosedTail(r, size, ref, digest); err != nil {
			return err
		}
		if !bytes.Equal(digest.Sum(nil), ref.SHA256[:]) {
			return ErrCorrupt
		}
		return nil
	}
	segment, err := OpenSegment(r, size)
	if err != nil {
		return err
	}
	if segment.Coverage() != ref.Coverage || segment.Count() != ref.Count {
		return ErrCorrupt
	}
	digest := sha256.New()
	var header [format.HeaderSize]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return err
	}
	_, _ = digest.Write(header[:])
	offset := int64(format.HeaderSize)
	// Verification retains no payloads; reuse stored bytes between blocks.
	var decoder format.Decoder
	defer decoder.Close()
	var stored []byte
	for _, block := range segment.blocks {
		if cap(stored) < int(block.size) {
			stored = make([]byte, int(block.size))
		} else {
			stored = stored[:int(block.size)]
		}
		if _, err := r.ReadAt(stored, int64(block.offset)); err != nil { // #nosec G115 -- OpenSegment validates block offsets within the int64 file size.
			return err
		}
		_, _ = digest.Write(stored)
		if err := walkBlockWithDecoder(block, stored, nil, &decoder); err != nil {
			return err
		}
		offset += int64(block.size)
	}
	// OpenSegment proved that blocks partition header..index and that index/footer
	// end exactly at size. This hashes all remaining bytes, including format-0
	// metadata, without retaining another complete index buffer.
	// Payload validation is complete, so its stored-byte buffer is available.
	// Keep metadata reads bounded without allocating 32 KiB for a small index.
	bufferSize := int(min(int64(32<<10), size-offset))
	if cap(stored) < bufferSize {
		stored = make([]byte, bufferSize)
	} else {
		stored = stored[:bufferSize]
	}
	if _, err := io.CopyBuffer(digest, io.NewSectionReader(r, offset, size-offset), stored); err != nil {
		return err
	}
	if !bytes.Equal(digest.Sum(nil), ref.SHA256[:]) {
		return ErrCorrupt
	}
	return nil
}
