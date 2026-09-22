package tidwall

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

var legacySegmentDecoder, _ = zstd.NewReader(nil)

// InspectLegacySegment validates a staged native file before adoption. Filename
// interpretation, decompression, and record boundaries belong to the backend;
// verify supplies the application's frame check. No files are modified.
func InspectLegacySegment(path string, verify func([]byte) error) (LegacySegment, error) {
	if verify == nil {
		return LegacySegment{}, eventlog.ErrInvalid
	}
	name := filepath.Base(path)
	seq, compressed, err := parseLegacySegmentName(name)
	if err != nil {
		return LegacySegment{}, err
	}
	if err := verifyLegacySegment(path, compressed, verify); err != nil {
		return LegacySegment{}, fmt.Errorf("refusing to adopt %s: %w", name, err)
	}
	return LegacySegment{Path: path, FirstSeq: seq, Compressed: compressed}, nil
}

func parseLegacySegmentName(name string) (uint64, bool, error) {
	base := name
	compressed := native.IsCompressedSegmentPath(base)
	if compressed {
		base = strings.TrimSuffix(base, ".zst")
	}
	if len(base) != 20 {
		return 0, false, fmt.Errorf("%q is not a segment file name", name)
	}
	seq, err := strconv.ParseUint(base, 10, 64)
	if err != nil || seq == 0 {
		return 0, false, fmt.Errorf("%q is not a segment file name", name)
	}
	return seq, compressed, nil
}

func verifyLegacySegment(path string, compressed bool, verify func([]byte) error) error {
	data, err := os.ReadFile(path) //nolint:gosec // G304: caller-owned staged native segment
	if err != nil {
		return err
	}
	if compressed {
		data, err = legacySegmentDecoder.DecodeAll(data, nil)
		if err != nil {
			return fmt.Errorf("compressed segment fails its zstd frame: %w", err)
		}
	}
	if len(data) == 0 {
		return errors.New("empty segment")
	}
	for off, ordinal := 0, 0; off < len(data); ordinal++ {
		size, n := binary.Uvarint(data[off:])
		if n <= 0 {
			return fmt.Errorf("incomplete record at offset %d", off)
		}
		remaining := len(data) - off - n
		if remaining < 0 || size > uint64(remaining) {
			return fmt.Errorf("incomplete record at offset %d", off)
		}
		length := int(size) //nolint:gosec // G115: bounded by remaining buffer length above
		if err := verify(data[off+n : off+n+length]); err != nil {
			return fmt.Errorf("record %d at offset %d: %w", ordinal, off, err)
		}
		off += n + length
	}
	return nil
}
