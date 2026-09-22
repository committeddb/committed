package wal

import "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"

// nativeEventTransferLocked composes native transport mechanics with the
// application's checksum validation and corruption metrics. Caller holds eventMu.
func (s *Storage) nativeEventTransferLocked() tidwall.LegacyTransfer {
	return s.eventLog.Transfer(func(raw []byte) ([]byte, error) {
		return s.unframe(raw, "event_log")
	})
}
