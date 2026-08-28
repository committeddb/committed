package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
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
		_, ferr := h.flushPending(context.Background(), true, nil)
		require.NoError(t, ferr)
		return <-ch
	}

	p0, p1 := flush(), flush()
	wantTS := int64(1_755_000_000) * int64(time.Second)
	require.Equal(t, wantTS, p0.SourceCommitUnixNano)
	require.Equal(t, wantTS, p1.SourceCommitUnixNano)
	require.Equal(t, "binlog.000007:900", p0.SourceTxnID)
	require.Equal(t, p0.SourceTxnID, p1.SourceTxnID, "multi-part flushes share the transaction identity")
	require.True(t, p0.TxnScopedDedup && p1.TxnScopedDedup,
		"this dialect bundles per transaction (handleXID), so every identity-stamped proposal opts into txn-scoped dedup")
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

// TestPinnedCoordinateTxns_DistinctIdentitiesAndBundledWatermarks is the
// dialect half of the compressed-transaction sub-collapse closure
// (unify-ingest-resume-dedup). Under binlog_transaction_compression=ON a
// whole multi-statement transaction dispatches under ONE outer coordinate
// (inner events carry end_log_pos 0), so a big transaction's soft-flush
// sub-index inflates its SourceSeqs far past the coordinate of a FOLLOWING
// small transaction. The old global-scalar dedup then silently dropped the
// follower. The fix is transaction scoping, and this pins the dialect's
// obligations to it: every part of each transaction carries that
// transaction's OWN GTID identity (distinct across the two), and each
// commit flush bundles a position whose GTID set covers the transaction —
// the watermark that commits atomically with the entities.
func TestPinnedCoordinateTxns_DistinctIdentitiesAndBundledWatermarks(t *testing.T) {
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	pr := make(chan *cluster.Proposal, 64) // > the 30 parts below: flushes must never block
	po := make(chan cluster.Position, 4)
	h := &MySQLEventHandler{
		proposalChan: pr,
		positionChan: po,
		curFile:      "binlog.000009",
		curPos:       5000, // the pinned outer coordinate for BOTH transactions
		flushBudget:  1,    // every row soft-flushes: the sub-inflation shape
	}

	beginTxn := func(gno string) {
		g, err := mysql.ParseMysqlGTIDSet(uuid + ":" + gno)
		require.NoError(t, err)
		h.curTxnGTID = g
		h.captureTxnID()
	}
	row := func(key string) {
		h.pending = append(h.pending, &cluster.Entity{Key: []byte(key), Data: []byte("v")})
		h.pendingBytes += 1000 // over the 1-byte budget: forces a soft flush
		if h.pendingBytes >= h.flushBudget {
			_, err := h.flushPending(context.Background(), true, nil)
			require.NoError(t, err)
		}
	}
	// finalRow buffers without soft-flushing, so the transaction's LAST part
	// rides the commit flush — the proposal the watermark bundles onto. (A
	// transaction whose every part soft-flushed has an empty commit flush and
	// checkpoints out-of-band instead, exactly as pre-bundling streaming did —
	// the torn-giant residual keeps its same-transaction part dedup either way.)
	finalRow := func(key string) {
		h.pending = append(h.pending, &cluster.Entity{Key: []byte(key), Data: []byte("v")})
	}

	// The BIG compressed transaction: 30 parts, ALL at its one pinned outer
	// coordinate (inner events carry end_log_pos 0), so the sub-index climbs
	// to 29 and the transaction's seqs reach 5000+29.
	beginTxn("100")
	for i := 0; i < 29; i++ {
		row(fmt.Sprintf("big-%d", i))
	}
	finalRow("big-29")
	require.NoError(t, h.handleXID(context.Background(), &replication.EventHeader{LogPos: 5000}))

	// The small follower's OWN outer coordinate sits just 10 bytes past the
	// big transaction's — far below the inflated 5000+29 highwater. This is
	// the collision: the follower's seq encodes BELOW seqs the big
	// transaction already used.
	h.curPos = 5010
	beginTxn("101")
	finalRow("small")
	require.NoError(t, h.handleXID(context.Background(), &replication.EventHeader{LogPos: 5010}))

	var big []*cluster.Proposal
	var small *cluster.Proposal
	for len(pr) > 0 {
		p := <-pr
		if p.SourceTxnID == uuid+":100" {
			big = append(big, p)
		} else {
			require.Equal(t, uuid+":101", p.SourceTxnID, "every proposal must carry its own transaction's identity")
			small = p
		}
	}
	require.Len(t, big, 30, "each soft-flushed part of the big transaction is one proposal")
	require.NotNil(t, small, "the follower transaction must be emitted")

	// The sub-collapse shape: the big transaction's inflated seqs exceed the
	// follower's — under a global scalar highwater the follower would be
	// silently dropped; transaction scoping is what admits it (pinned at the
	// core by TestTxnDedup_DifferentTxnIsNeverComparedAcrossTxns).
	maxBig := big[len(big)-1].SourceSeq
	require.Greater(t, maxBig, small.SourceSeq,
		"the inflated big-transaction highwater must exceed the follower's seq — the collision this fix defuses")

	// Watermark bundling: each commit flush carries the post-transaction
	// position, whose GTID set covers that transaction (and, for the
	// follower, its predecessor).
	requireBundledGTID := func(p *cluster.Proposal, gnos ...string) {
		t.Helper()
		require.NotEmpty(t, p.Position, "the commit flush must bundle the watermark")
		var pp dialectpb.MySQLBinLogPosition
		require.NoError(t, proto.Unmarshal(p.Position, &pp))
		set, err := mysql.ParseMysqlGTIDSet(pp.GetGtidSet())
		require.NoError(t, err)
		for _, gno := range gnos {
			one, err := mysql.ParseMysqlGTIDSet(uuid + ":" + gno)
			require.NoError(t, err)
			require.Truef(t, set.Contain(one), "watermark %q must cover txn %s", pp.GetGtidSet(), gno)
		}
	}
	// Soft-flushed parts never carry a position; only each txn's final flush does.
	for _, p := range big[:len(big)-1] {
		require.Empty(t, p.Position, "mid-transaction parts must not advance the watermark")
	}
	requireBundledGTID(big[len(big)-1], "100")
	requireBundledGTID(small, "100", "101")
}
