package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// Direct native storage access uses physical sequences and native files.
// Shared backends receive those files by importing their logical records.
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
