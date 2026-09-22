package tidwall

import (
	"encoding/binary"
	"fmt"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacySegment and LegacyLayout describe the native peer/backup format, not
// the backend-neutral EventLog contract. The owner pins the layout while using
// these paths and excludes replacement/close while taking a snapshot.
type LegacySegment struct {
	Path       string
	FirstSeq   uint64
	Compressed bool
}

type LegacyLayout struct {
	Sealed       []LegacySegment
	TailPath     string
	TailFirstSeq uint64
	TailLen      int64
	LastSeq      uint64
}

// LegacyTransfer preserves native framing. The application supplies checksum
// and envelope decoding, which must not mutate bytes. The owner excludes truncation,
// replacement, and close during calls and owns any longer file-read lifetime.
type LegacyTransfer struct {
	log         *native.Log
	DecodeFrame func([]byte) ([]byte, error)
}

func (t LegacyTransfer) Layout() (LegacyLayout, error) {
	if t.log == nil {
		return LegacyLayout{}, eventlog.ErrInvalid
	}
	layout, err := t.log.LayoutSnapshot()
	if err != nil {
		return LegacyLayout{}, err
	}
	out := LegacyLayout{TailPath: layout.Tail.Path, TailFirstSeq: layout.Tail.Index, TailLen: layout.TailLen, LastSeq: layout.LastIndex}
	for _, s := range layout.Sealed {
		out.Sealed = append(out.Sealed, LegacySegment{Path: s.Path, FirstSeq: s.Index, Compressed: native.IsCompressedSegmentPath(s.Path)})
	}
	return out, nil
}

// Read verifies and returns one native frame. Byte ownership follows the native
// handle's options; callers using NoCopy must consume it before invalidation.
func (t LegacyTransfer) Read(sequence uint64) ([]byte, error) {
	if t.log == nil || t.DecodeFrame == nil {
		return nil, eventlog.ErrInvalid
	}
	raw, err := t.log.Read(sequence)
	if err != nil {
		return nil, err
	}
	if _, err := t.DecodeFrame(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// EncodeRecords emits length-prefixed native frames over inclusive sequences.
// The byte budget is checked after each complete record, so one oversized record
// makes progress. Returned bytes are owned by the caller.
func (t LegacyTransfer) EncodeRecords(lo, hi uint64, maxBytes int) (data []byte, last uint64, err error) {
	for seq := lo; seq <= hi; seq++ {
		raw, err := t.Read(seq)
		if err != nil {
			return nil, 0, fmt.Errorf("event log read seq %d to serve: %w", seq, err)
		}
		data = binary.AppendUvarint(data, uint64(len(raw)))
		data = append(data, raw...)
		last = seq
		if len(data) >= maxBytes || seq == hi {
			break
		}
	}
	return data, last, nil
}

// ReadPayload verifies and strips the application envelope once. The returned
// payload may alias native bytes; the owner consumes it within its read lifetime.
func (t LegacyTransfer) ReadPayload(sequence uint64) ([]byte, error) {
	if t.log == nil || t.DecodeFrame == nil {
		return nil, eventlog.ErrInvalid
	}
	raw, err := t.log.Read(sequence)
	if err != nil {
		return nil, err
	}
	return t.DecodeFrame(raw)
}

// FirstSequence and LastSequence expose native positions only to native-format
// maintenance and transfer operations. Zero denotes an empty log.
func (t LegacyTransfer) FirstSequence() (uint64, error) {
	if t.log == nil {
		return 0, eventlog.ErrInvalid
	}
	return t.log.FirstIndex()
}

func (t LegacyTransfer) LastSequence() (uint64, error) {
	if t.log == nil {
		return 0, eventlog.ErrInvalid
	}
	return t.log.LastIndex()
}
