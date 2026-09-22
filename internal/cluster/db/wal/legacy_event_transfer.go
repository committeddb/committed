package wal

import "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"

// nativeEventTransferLocked composes native transport mechanics with the
// application's checksum validation and corruption metrics. Caller holds eventMu.
func (s *Storage) nativeEventTransferLocked() tidwall.LegacyTransfer {
	return tidwall.LegacyTransfer{Log: s.eventLog, DecodeFrame: func(raw []byte) ([]byte, error) {
		return s.unframe(raw, "event_log")
	}}
}
