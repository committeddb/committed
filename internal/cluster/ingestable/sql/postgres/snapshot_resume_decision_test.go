package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

// TestNeedsColdSnapshot pins the snapshot-vs-stream resume decision, whose bug
// silently skipped the initial snapshot when a worker froze during it before its
// first batch checkpointed: the replication slot outlives a committed-side
// restart, so the old `slotIsNew`-keyed decision read a pre-existing slot as
// "already snapshotted" and streamed an empty topic. needsColdSnapshot keys on
// committed's durable position instead (mirroring MySQL's lastPos==nil), using
// refresh_epoch to tell "never checkpointed a batch" (epoch==0) from
// "completed but idle" (epoch>=1, lsn still 0).
//
// Mid-snapshot resume (resumeProgress != nil) is handled by the caller's
// `|| *resumeProgress != nil`, not this predicate, so it is not covered here.
//
// Red proof: reverting needsColdSnapshot to `slotIsNew && lastLSN == 0` flips the
// "froze before first batch" case to false — the silent skip — failing this test.
func TestNeedsColdSnapshot(t *testing.T) {
	const streamingLSN = pglogrepl.LSN(0x16B3748)

	for _, tc := range []struct {
		name      string
		slotIsNew bool
		lastLSN   pglogrepl.LSN
		epoch     uint64
		want      bool
	}{
		{"cold start: fresh slot, empty position", true, 0, 0, true},
		{"froze before first batch: slot exists, empty position", false, 0, 0, true}, // the fix
		{"completed but idle: slot exists, snapshot done, no stream yet", false, 0, 1, false},
		{"streaming: new-format position", false, streamingLSN, 1, false},
		{"legacy streaming: 8-byte position, pre-epoch", false, streamingLSN, 0, false},
		{"recreated slot with surviving completed-idle position", true, 0, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := needsColdSnapshot(tc.slotIsNew, tc.lastLSN, tc.epoch); got != tc.want {
				t.Fatalf("needsColdSnapshot(slotIsNew=%v, lastLSN=%d, epoch=%d) = %v, want %v",
					tc.slotIsNew, tc.lastLSN, tc.epoch, got, tc.want)
			}
		})
	}
}
