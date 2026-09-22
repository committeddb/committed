package wal

import "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"

// nativeEventTransferLocked composes native transport mechanics with the
// application's checksum validation and corruption metrics. Caller holds eventMu.
func (s *Storage) nativeEventTransferLocked() tidwall.LegacyTransfer {
	return tidwall.LegacyTransfer{Log: s.eventLog, Verify: func(raw []byte) error {
		_, err := s.unframe(raw, "event_log")
		return err
	}}
}
