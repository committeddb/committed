package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"io"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// verifySegmentDigest retains full file and semantic verification while reading
// stored payload blocks only once. The ReaderAt must remain immutable, as for
// OpenSegment. Metadata is first validated by OpenSegment, then read in file
// order for the whole-file digest; only one bounded stored/decoded block is live.
func verifySegmentDigest(r io.ReaderAt, size int64, ref SegmentRef) error {
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
	for _, block := range segment.blocks {
		stored := make([]byte, int(block.size))
		if _, err := r.ReadAt(stored, int64(block.offset)); err != nil { // #nosec G115 -- OpenSegment validates block offsets within the int64 file size.
			return err
		}
		_, _ = digest.Write(stored)
		if err := walkBlock(block, stored, nil); err != nil {
			return err
		}
		offset += int64(block.size)
	}
	// OpenSegment proved that blocks partition header..index and that index/footer
	// end exactly at size. This hashes all remaining bytes, including format-0
	// metadata, without retaining another complete index buffer.
	if _, err := io.Copy(digest, io.NewSectionReader(r, offset, size-offset)); err != nil {
		return err
	}
	if !bytes.Equal(digest.Sum(nil), ref.SHA256[:]) {
		return ErrCorrupt
	}
	return nil
}
