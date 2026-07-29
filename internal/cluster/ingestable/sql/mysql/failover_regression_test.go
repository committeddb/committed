package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFlushPending_FlagsFailoverLineageRegression pins the failover half of
// mysql-samecoordinate-dedup-stability (F2): when the live binlog coordinate's
// file number drops below the resume baseline — a promoted replica whose
// numbering restarts low, so its coordinates encode below the durable SourceSeq
// highwater — the emitted proposal must be flagged DedupUnsafe so the ingest
// worker freezes instead of silently dedup-dropping real post-failover data.
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
		require.NoError(t, h.flushPending(context.Background(), false))
		p := <-ch
		require.True(t, p.DedupUnsafe,
			"a coordinate below the resume baseline must be flagged DedupUnsafe")
	})

	t.Run("forward coordinate is not flagged", func(t *testing.T) {
		// Same lineage, file rotated forward — the normal case.
		h, ch := newHandler("binlog.000047", "binlog.000048")
		require.NoError(t, h.flushPending(context.Background(), false))
		p := <-ch
		require.False(t, p.DedupUnsafe,
			"a forward coordinate must not be flagged, or every ordinary flush would freeze")
	})

	t.Run("no baseline never flags", func(t *testing.T) {
		// An unparseable resume file name yields resumeFileNum 0 (dedup already
		// disabled for such coordinates) — it must never trip the guard.
		h, ch := newHandler("mysql-bin", "binlog.000001")
		require.NoError(t, h.flushPending(context.Background(), false))
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
		require.NoError(t, h.flushPending(context.Background(), false))
		p := <-ch
		require.True(t, p.DedupUnsafe,
			"same file ordinal at a lower offset encodes below the highwater and must be flagged, not silently dropped")
	})

	t.Run("equal ordinal, higher offset is not flagged (normal forward progress)", func(t *testing.T) {
		h, ch := sameOrdinal(8_000_000, 9_000_000) // same lineage, advanced within the file
		require.NoError(t, h.flushPending(context.Background(), false))
		p := <-ch
		require.False(t, p.DedupUnsafe,
			"same-lineage forward progress within the resume file must not freeze")
	})
}
