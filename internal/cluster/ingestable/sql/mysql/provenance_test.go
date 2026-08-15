package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestCaptureTxnID covers the transaction-identity capture: GTID when the
// source announces one, the binlog coordinate as the gtid_mode=OFF fallback,
// and capture-once semantics (a multi-part transaction's later rows — even
// ones observed after a late GTID — never rewrite the identity its first
// buffered row established).
func TestCaptureTxnID(t *testing.T) {
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	gtid, err := mysql.ParseMysqlGTIDSet(uuid + ":23")
	require.NoError(t, err)

	// gtid_mode=ON: the GTIDEvent preceded the rows, so the identity is the GTID.
	h := &MySQLEventHandler{curFile: "binlog.000007", curPos: 900, curTxnGTID: gtid}
	h.captureTxnID()
	require.Equal(t, uuid+":23", h.curTxnID)

	// gtid_mode=OFF: fall back to the first row's binlog coordinate.
	h = &MySQLEventHandler{curFile: "binlog.000007", curPos: 900}
	h.captureTxnID()
	require.Equal(t, "binlog.000007:900", h.curTxnID)

	// Capture-once: a later coordinate (or a GTID appearing afterwards) never
	// rewrites the identity — every part of the transaction shares it.
	h.curPos = 1500
	h.curTxnGTID = gtid
	h.captureTxnID()
	require.Equal(t, "binlog.000007:900", h.curTxnID)
}

// TestFlushPendingStampsProvenance proves every proposal flushed for one
// transaction — including repeated flushes at one coordinate, the multi-part
// case — carries the transaction's commit timestamp and shared identity.
func TestFlushPendingStampsProvenance(t *testing.T) {
	ch := make(chan *cluster.Proposal, 8)
	h := &MySQLEventHandler{
		proposalChan:   ch,
		curFile:        "binlog.000007",
		curPos:         900,
		curTxnSourceTS: 1_755_000_000,
		curTxnID:       "binlog.000007:900",
	}

	flush := func() *cluster.Proposal {
		h.pending = []*cluster.Entity{{Key: []byte("k")}}
		require.NoError(t, h.flushPending(context.Background(), true))
		return <-ch
	}

	p0, p1 := flush(), flush()
	wantTS := int64(1_755_000_000) * int64(time.Second)
	require.Equal(t, wantTS, p0.SourceCommitUnixNano)
	require.Equal(t, wantTS, p1.SourceCommitUnixNano)
	require.Equal(t, "binlog.000007:900", p0.SourceTxnID)
	require.Equal(t, p0.SourceTxnID, p1.SourceTxnID, "multi-part flushes share the transaction identity")
}

// TestHandleXIDStampsCommitTimeAndClearsProvenance proves the commit flush
// carries the XID event's header timestamp — the source commit time — and
// that commit clears the provenance so it cannot bleed into the next
// transaction.
func TestHandleXIDStampsCommitTimeAndClearsProvenance(t *testing.T) {
	pr := make(chan *cluster.Proposal, 4)
	po := make(chan cluster.Position, 4)
	h := &MySQLEventHandler{
		proposalChan:   pr,
		positionChan:   po,
		curFile:        "binlog.000007",
		curPos:         900,
		curTxnSourceTS: 1_755_000_000, // a RowsEvent's earlier statement time
		curTxnID:       "binlog.000007:900",
		pending:        []*cluster.Entity{{Key: []byte("k")}},
	}

	header := &replication.EventHeader{Timestamp: 1_755_000_002, LogPos: 1000}
	require.NoError(t, h.handleXID(context.Background(), header))

	p := <-pr
	require.Equal(t, int64(1_755_000_002)*int64(time.Second), p.SourceCommitUnixNano,
		"the commit flush stamps the XID header's timestamp, not the earlier statement time")
	require.Equal(t, "binlog.000007:900", p.SourceTxnID)

	require.Zero(t, h.curTxnSourceTS, "commit clears the transaction's timestamp")
	require.Empty(t, h.curTxnID, "commit clears the transaction's identity")
}
