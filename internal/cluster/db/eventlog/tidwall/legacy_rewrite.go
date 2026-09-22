package tidwall

import (
	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacyRewrite writes an unpublished native replacement with dense sequences.
// It has one owner; calls are not concurrent. The owner supplies already-framed
// survivors and owns the private directory, publication, and failure cleanup.
// Per-record syncing is disabled; Sync must succeed before publication.
type LegacyRewrite struct {
	log   *native.Log
	count uint64
}

// CreateLegacyRewrite opens the caller's fresh private rewrite directory with
// native sealed-segment compression enabled. The directory must contain no log.
func CreateLegacyRewrite(path string) (*LegacyRewrite, error) {
	log, err := native.Open(path, &native.Options{NoSync: true, SealedSegmentCompression: native.CompressionZstd})
	if err != nil {
		return nil, err
	}
	// A rewrite cannot append to an existing generation accidentally.
	last, err := log.LastIndex()
	if err != nil || last != 0 {
		_ = log.Close()
		if err != nil {
			return nil, err
		}
		return nil, eventlog.ErrInvalid
	}
	return &LegacyRewrite{log: log}, nil
}

func (w *LegacyRewrite) Append(frame []byte) error {
	if w.count == ^uint64(0) {
		return eventlog.ErrInvalid
	}
	if err := w.log.Write(w.count+1, frame); err != nil {
		return err
	}
	w.count++
	return nil
}

func (w *LegacyRewrite) Count() uint64 { return w.count }
func (w *LegacyRewrite) Sync() error   { return w.log.Sync() }
func (w *LegacyRewrite) Close() error  { return w.log.Close() }

// CompressSealed prepares the bulk replacement before the publication lock.
// The final appended delta may seal more segments; the normal sealer handles
// those after publication.
func (w *LegacyRewrite) CompressSealed() error {
	for {
		did, err := w.log.CompressNextSealed()
		if err != nil {
			return err
		}
		if !did {
			return nil
		}
	}
}
