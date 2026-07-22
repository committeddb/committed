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
}
