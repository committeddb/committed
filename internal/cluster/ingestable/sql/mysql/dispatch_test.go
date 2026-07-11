package mysql

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestDispatchEvent_ExpandsCompressedTransactionPayload is the
// mysql-cdc-transaction-compression regression. With
// binlog_transaction_compression=ON (GA since MySQL 8.0.20, common on managed
// MySQL), go-mysql delivers each transaction's TableMap/Rows/XID events wrapped
// in one *replication.TransactionPayloadEvent that it decodes but does NOT
// auto-expand. runStream's event switch had no case for it (and no default), so
// every compressed transaction was silently dropped — no rows, and the XID never
// fired, so the binlog position never advanced (a stalled, silent-data-loss
// ingest). dispatchEvent must expand the payload and route each sub-event through
// the same handlers, using the OUTER event's position (inner sub-events carry
// end_log_pos == 0).
func TestDispatchEvent_ExpandsCompressedTransactionPayload(t *testing.T) {
	posCh := make(chan cluster.Position, 1)
	h := &MySQLEventHandler{
		positionChan: posCh,
		curFile:      "binlog.000007",
	}

	// The real commit offset is the outer event's LogPos; the inner sub-event's
	// LogPos is 0, as MySQL writes it inside a compressed payload.
	const outerPos = uint32(4096)
	payload := &replication.TransactionPayloadEvent{
		Events: []*replication.BinlogEvent{
			{Header: &replication.EventHeader{LogPos: 0}, Event: &replication.XIDEvent{}},
		},
	}
	outer := &replication.EventHeader{LogPos: outerPos}

	require.NoError(t, h.dispatchEvent(context.Background(), outer, payload))

	select {
	case bs := <-posCh:
		var pp dialectpb.MySQLBinLogPosition
		require.NoError(t, proto.Unmarshal(bs, &pp))
		require.Equal(t, "binlog.000007", pp.GetName())
		require.Equal(t, outerPos, pp.GetPos(),
			"the commit position must be the outer payload offset, not the inner sub-event's 0")
	default:
		t.Fatal("compressed transaction was silently dropped: the wrapped XID never checkpointed a position")
	}
}
