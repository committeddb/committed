package tidwall

import native "github.com/tidwall/wal"

// LegacyLog owns one production-format native handle. Capabilities obtained from
// it belong to this handle's lifetime; directory replacement creates a new owner.
// The caller excludes replacement/close while using readers and writers. Native
// operations retain their own synchronization; this owner adds no read-path lock.
// The underlying handle is deliberately not exposed.
type LegacyLog struct {
	log *native.Log
}

// OwnLegacy transfers ownership of an existing native handle. The caller must
// not close it independently or create another owner for the same handle.
func OwnLegacy(log *native.Log) *LegacyLog { return &LegacyLog{log: log} }

func (l *LegacyLog) Close() error { return l.log.Close() }

// NewAppender binds application framing and identity to this handle.
func (l *LegacyLog) NewAppender(codec LegacyCodec) *LegacyAppender {
	return NewLegacyAppender(l.log, codec)
}

// NewLegacyLogCursor carries the application's decoded value through native
// positioning without copying or decoding it again.
func NewLegacyLogCursor[T any](l *LegacyLog, decode func([]byte) (uint64, T, error)) *LegacyCursor[T] {
	return NewLegacyCursor(l.log, decode)
}

func (l *LegacyLog) Transfer(decodeFrame func([]byte) ([]byte, error)) LegacyTransfer {
	return LegacyTransfer{log: l.log, DecodeFrame: decodeFrame}
}

func (l *LegacyLog) CompressNextSealed() (bool, error) {
	return (LegacyCompression{Log: l.log}).CompressNextSealed()
}

// SegmentCacheSize reports the effective native cache setting for diagnostics.
func (l *LegacyLog) SegmentCacheSize() int { return l.log.SegmentCacheSize() }
