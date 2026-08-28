package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFlushPending_FlagsFailoverLineageRegression pins the failover-lineage
// guard for a FILE:POS-ONLY session (gtidResume false — a gtid_mode=OFF
// source or legacy checkpoint): when the live binlog coordinate drops to or
// below the resume baseline — a promoted replica whose numbering restarts
// low — the emitted proposal must be flagged DedupUnsafe so the ingest
// worker freezes instead of silently dedup-dropping real post-failover
// data. Coordinates are this session's only identity, so the freeze is the
// only safe answer. (A GTID-resume session stamps LineageRegressed instead
// and rides through — see TestFlushPending_GTIDResumeLineageRegression.)
//
// The control case (a forward coordinate, the normal same-lineage stream) must
// NOT be flagged, or every ordinary flush would freeze.
func TestFlushPending_FlagsFailoverLineageRegression(t *testing.T) {
	// flushPending sends on a send-only field; keep a bidirectional handle to read.
	newHandler := func(resumeFile, curFile string) (*MySQLEventHandler, chan *cluster.Proposal) {
		ch := make(chan *cluster.Proposal, 1)
		rn, _ := binlogFileNum(resumeFile)
		h := &MySQLEventHandler{
			proposalChan:  ch,
			resumeFileNum: rn,
			curFile:       curFile,
			curPos:        100,
			pending:       []*cluster.Entity{{Key: []byte("k"), Data: []byte("v")}},
		}
		return h, ch
	}

	t.Run("regression is flagged", func(t *testing.T) {
		// Resumed from binlog.000047; now streaming binlog.000001 (a promoted
		// replica). Its coordinate encodes below the old highwater.
		h, ch := newHandler("binlog.000047", "binlog.000001")
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.True(t, p.DedupUnsafe,
			"a coordinate below the resume baseline must be flagged DedupUnsafe")
	})

	t.Run("forward coordinate is not flagged", func(t *testing.T) {
		// Same lineage, file rotated forward — the normal case.
		h, ch := newHandler("binlog.000047", "binlog.000048")
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.False(t, p.DedupUnsafe,
			"a forward coordinate must not be flagged, or every ordinary flush would freeze")
	})

	t.Run("no baseline never flags", func(t *testing.T) {
		// An unparseable resume file name yields resumeFileNum 0 (dedup already
		// disabled for such coordinates) — it must never trip the guard.
		h, ch := newHandler("mysql-bin", "binlog.000001")
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.False(t, p.DedupUnsafe, "no numeric baseline must never flag")
	})

	// A failover to a promoted replica whose current binlog coincidentally shares
	// the resume file ORDINAL but sits at a LOWER offset. The old file-number-only
	// guard (`n < resumeFileNum`) missed this — n == resumeFileNum — so the
	// coordinate encoded below the highwater and genuinely-new data was SILENTLY
	// dedup-dropped (SourceSeq = fileNum<<32 | pos). The full-coordinate guard
	// must flag it. This is the silent-LOSS case, strictly worse than the < case.
	sameOrdinal := func(resumePos, curPos uint32) (*MySQLEventHandler, chan *cluster.Proposal) {
		ch := make(chan *cluster.Proposal, 1)
		rn, _ := binlogFileNum("binlog.000042")
		h := &MySQLEventHandler{
			proposalChan:  ch,
			resumeFileNum: rn,
			resumePos:     resumePos,
			curFile:       "binlog.000042",
			curPos:        curPos,
			pending:       []*cluster.Entity{{Key: []byte("k"), Data: []byte("v")}},
		}
		return h, ch
	}

	t.Run("equal ordinal, lower offset is flagged (silent-loss case)", func(t *testing.T) {
		h, ch := sameOrdinal(8_000_000, 500_000) // resumed at 8M; replica at 500k
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.True(t, p.DedupUnsafe,
			"same file ordinal at a lower offset encodes below the highwater and must be flagged, not silently dropped")
	})

	t.Run("equal ordinal, higher offset is not flagged (normal forward progress)", func(t *testing.T) {
		h, ch := sameOrdinal(8_000_000, 9_000_000) // same lineage, advanced within the file
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.False(t, p.DedupUnsafe,
			"same-lineage forward progress within the resume file must not freeze")
	})
}

// TestFlushPending_GTIDResumeLineageRegression pins the ride-through half:
// a session that resumed by the GTID-set watermark (gtidResume) stamps a
// coordinate regression LineageRegressed — NOT DedupUnsafe — because the
// transaction-scoped dedup record makes a new lineage's low coordinates
// harmless (the worker freezes only while the stored record is still the
// legacy scalar regime; see cluster.Proposal.LineageRegressed).
func TestFlushPending_GTIDResumeLineageRegression(t *testing.T) {
	newHandler := func(curFile string) (*MySQLEventHandler, chan *cluster.Proposal) {
		ch := make(chan *cluster.Proposal, 1)
		rn, _ := binlogFileNum("binlog.000047")
		h := &MySQLEventHandler{
			proposalChan:  ch,
			resumeFileNum: rn,
			gtidResume:    true,
			curFile:       curFile,
			curPos:        100,
			pending:       []*cluster.Entity{{Key: []byte("k"), Data: []byte("v")}},
		}
		return h, ch
	}

	t.Run("regression stamps LineageRegressed, not DedupUnsafe", func(t *testing.T) {
		h, ch := newHandler("binlog.000001")
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.True(t, p.LineageRegressed, "a GTID-resume regression must stamp the ride-through hint")
		require.False(t, p.DedupUnsafe, "a GTID-resume regression must not freeze a healthy failover")
	})

	t.Run("forward coordinate stamps neither", func(t *testing.T) {
		h, ch := newHandler("binlog.000048")
		_, err := h.flushPending(context.Background(), false, nil)
		require.NoError(t, err)
		p := <-ch
		require.False(t, p.LineageRegressed)
		require.False(t, p.DedupUnsafe)
	})
}

// TestFlushPending_BundlesPositionOnFinalGroup pins the watermark-atomicity
// mechanics: the commit flush attaches the encoded post-transaction position
// to the LAST emitted proposal (position and entities commit in one raft
// entry), and a soft flush never carries one (a position must not advance
// past an uncommitted transaction).
func TestFlushPending_BundlesPositionOnFinalGroup(t *testing.T) {
	ch := make(chan *cluster.Proposal, 4)
	h := &MySQLEventHandler{
		proposalChan: ch,
		curFile:      "binlog.000002",
		curPos:       500,
		pending:      []*cluster.Entity{{Key: []byte("k"), Data: []byte("v")}},
	}
	pos := []byte("encoded-position")
	emitted, err := h.flushPending(context.Background(), false, pos)
	require.NoError(t, err)
	require.True(t, emitted)
	p := <-ch
	require.Equal(t, cluster.Position(pos), p.Position, "the commit flush must bundle the watermark")

	h.pending = []*cluster.Entity{{Key: []byte("k2"), Data: []byte("v2")}}
	emitted, err = h.flushPending(context.Background(), true, nil)
	require.NoError(t, err)
	require.True(t, emitted)
	p = <-ch
	require.Empty(t, p.Position, "a soft flush must never advance the position")

	h.pending = nil
	emitted, err = h.flushPending(context.Background(), false, pos)
	require.NoError(t, err)
	require.False(t, emitted, "an empty flush reports nothing emitted so handleXID checkpoints out-of-band")
}
