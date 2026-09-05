package wal

import (
	"errors"
	"time"

	"github.com/tidwall/wal"
	"go.uber.org/zap"
)

// The background sealer drives sealed-segment compression on the permanent
// event log: it repeatedly asks the log to compress its oldest plain sealed
// segment until none remain, then idles and re-checks as the log grows. One
// mechanism covers both steady state (each segment compresses shortly after
// the log cycles past it) and the day-one backfill (a pre-0.8.0 log's whole
// backlog compresses after the upgrade, oldest first, ~9x smaller at rest —
// measured L3 zstd on a 37 GB field log: 4.2 GB).
//
// The write path never compresses: the expensive encode runs in this
// goroutine against immutable sealed files, off the raft Ready loop, with a
// pacing sleep between segments so a large backfill trickles rather than
// saturating disk I/O next to live traffic.

const (
	// sealerIdleInterval is the default for how often the sealer re-checks
	// when everything is compressed (Storage.sealerIdle; tests shorten it
	// via WithSealerIdleInterval). A 20 MB segment seals every ~20 MB of
	// writes, so a fresh segment waits at most this long for compression —
	// bytes at rest, no correctness stake.
	sealerIdleInterval = 30 * time.Second
	// sealerPace separates consecutive segment compressions (each ~20 MB
	// read + ~40 ms encode + ~2-3 MB write) so a multi-thousand-segment
	// backfill shares the disk with live traffic.
	sealerPace = 100 * time.Millisecond
	// sealerErrorBackoff spaces retries after an error (an ErrClosed during
	// the scrub swap window being the expected one — the reopened log is
	// re-fetched on the next pass).
	sealerErrorBackoff = 5 * time.Second
)

// sealerWorker runs until stopSealer. It re-fetches the event-log handle
// under eventMu every iteration because the scrub swap replaces it.
func (s *Storage) sealerWorker() {
	defer close(s.sealerDone)
	compressed := 0
	logged := false
	wait := func(d time.Duration) bool {
		select {
		case <-s.sealerStop:
			return false
		case <-time.After(d):
			return true
		}
	}
	for {
		select {
		case <-s.sealerStop:
			return
		default:
		}

		s.eventMu.RLock()
		log := s.eventLog
		s.eventMu.RUnlock()

		did, err := log.CompressNextSealed()
		switch {
		case err != nil:
			// ErrClosed is the scrub-swap window (the handle we fetched was
			// retired) — benign, re-fetch after a backoff. Anything else is
			// logged and retried on the same cadence: compression is a
			// bytes-at-rest concern and must never wedge the node.
			if !errors.Is(err, wal.ErrClosed) {
				s.logger.Warn("event-log sealer: compression attempt failed; will retry",
					zap.Error(err))
			}
			if !wait(sealerErrorBackoff) {
				return
			}
		case did:
			compressed++
			logged = false
			if compressed%100 == 0 {
				s.logger.Info("event-log sealer: compressing sealed segments",
					zap.Int("compressed_since_start", compressed))
			}
			if !wait(sealerPace) {
				return
			}
		default:
			if compressed > 0 && !logged {
				s.logger.Info("event-log sealer: all sealed segments compressed",
					zap.Int("compressed_since_start", compressed))
				logged = true
			}
			if !wait(s.sealerIdle) {
				return
			}
		}
	}
}

// stopSealer signals the sealer and waits for it to return, so no
// compression swap is mid-flight when the event log closes. Idempotent.
func (s *Storage) stopSealer() {
	s.sealerStopOnce.Do(func() {
		close(s.sealerStop)
		<-s.sealerDone
	})
}
