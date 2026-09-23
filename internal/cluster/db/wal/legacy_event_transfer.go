package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// Native transfer uses physical sequences, framed records, and native files.
// A shared logical log does not imply support for that on-disk protocol.
func (s *Storage) requireNativeEventLog() error {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.requireNativeEventLogLocked()
}

func (s *Storage) requireNativeEventLogLocked() error {
	if s.eventLog.native == nil {
		return eventlog.ErrUnsupported
	}
	return nil
}

// nativeEventTransferLocked composes native transport mechanics with the
// application's checksum validation and corruption metrics. Caller holds eventMu.
func (s *Storage) nativeEventTransferLocked() tidwall.LegacyTransfer {
	return s.eventLog.native.Transfer(func(raw []byte) ([]byte, error) {
		return s.unframe(raw, "event_log")
	})
}
