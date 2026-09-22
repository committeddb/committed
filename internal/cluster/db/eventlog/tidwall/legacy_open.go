package tidwall

import (
	"errors"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacyOptions configures opening the production native event log. Zero values
// use the native cache and segment-size defaults. Sealed segments use zstd;
// appends remain synchronous and the active tail remains uncompressed.
// Keep the same options when reopening after a directory replacement.
type LegacyOptions struct {
	SegmentCacheSize int
	SegmentSize      int
}

// OpenLegacy opens the existing production format, without a CURRENT file or
// logical-ID envelope. The caller owns the returned handle and coordinates its
// lifetime with readers, writers, and directory replacement.
func OpenLegacy(path string, opts LegacyOptions) (*LegacyLog, error) {
	log, err := native.Open(path, &native.Options{
		SegmentCacheSize:         opts.SegmentCacheSize,
		SegmentSize:              opts.SegmentSize,
		SealedSegmentCompression: native.CompressionZstd,
	})
	if errors.Is(err, native.ErrCorrupt) {
		return nil, errors.Join(eventlog.ErrCorrupt, err)
	}
	if err != nil {
		return nil, err
	}
	return OwnLegacy(log), nil
}
