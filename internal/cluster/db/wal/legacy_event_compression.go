package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// eventCompressorLocked captures the native compression capability under eventMu.
// A step runs outside eventMu, as before; a concurrently retired handle reports
// ErrClosed, and the worker binds to the current handle on its next iteration.
func (s *Storage) eventCompressorLocked() eventlog.SealedCompressor {
	return tidwall.LegacyCompression{Log: s.eventLog}
}
