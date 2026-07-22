package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestEncodeSourceSeq verifies the (file, offset, sub-index) -> uint64 mapping is
// strictly monotonic in binlog order and degrades safely to 0 on an unparseable
// file name (which disables dedup for that proposal rather than risking a
// false-positive skip).
func TestEncodeSourceSeq(t *testing.T) {
	// Within a file, a larger offset is a larger seq.
	if a, b := encodeSourceSeq("binlog.000042", 100, 0), encodeSourceSeq("binlog.000042", 200, 0); a >= b {
		t.Fatalf("within-file: encode(.42,100)=%d should be < encode(.42,200)=%d", a, b)
	}

	// A later file outranks an earlier one regardless of offset — the rotation
	// case (earlier file at a huge offset vs new file at offset 4).
	if a, b := encodeSourceSeq("binlog.000041", 1<<31, 0), encodeSourceSeq("binlog.000042", 4, 0); a >= b {
		t.Fatalf("rotation: encode(.41,big)=%d should be < encode(.42,4)=%d", a, b)
	}

	// sub==0 is exactly the bare coordinate, so a single-flush event keeps its
	// prior seq (a persisted highwater stays valid across upgrade).
	require.Equal(t, uint64(42)<<32|500, encodeSourceSeq("binlog.000042", 500, 0))

	// The sub-index strictly increases the seq at a fixed coordinate — the partial
	// flushes of one oversized RowsEvent.
	s0 := encodeSourceSeq("binlog.000042", 500, 0)
	s1 := encodeSourceSeq("binlog.000042", 500, 1)
	s2 := encodeSourceSeq("binlog.000042", 500, 2)
	require.True(t, s0 < s1 && s1 < s2, "sub-index must strictly increase: %d, %d, %d", s0, s1, s2)

	// A sub-index never reaches the NEXT event's seq: consecutive binlog events
	// differ by >= the minimum event size (~19 bytes, a header), which exceeds any
	// one RowsEvent's flush count. So sub-indices stay within their coordinate's
	// gap and never collide with the following event.
	if a, b := encodeSourceSeq("binlog.000042", 1000, 3), encodeSourceSeq("binlog.000042", 1019, 0); a >= b {
		t.Fatalf("sub-index must not reach the next event: encode(1000,sub3)=%d should be < encode(1019,sub0)=%d", a, b)
	}

	// Monotonic across a longer sequence of coordinates.
	prev := uint64(0)
	coords := []struct {
		file string
		pos  uint32
	}{
		{"mysql-bin.000001", 4},
		{"mysql-bin.000001", 9999},
		{"mysql-bin.000002", 4},
		{"mysql-bin.000010", 120},
		{"mysql-bin.000100", 1},
	}
	for _, c := range coords {
		got := encodeSourceSeq(c.file, c.pos, 0)
		if got <= prev {
			t.Fatalf("non-monotonic at %s:%d -> %d (prev %d)", c.file, c.pos, got, prev)
		}
		prev = got
	}

	// Unparseable file names map to 0 (dedup disabled, never a false skip).
	for _, bad := range []string{"", "noNumber", "binlog.", "binlog.abc", "trailingdot."} {
		if got := encodeSourceSeq(bad, 123, 0); got != 0 {
			t.Fatalf("encode(%q,123) = %d, want 0", bad, got)
		}
	}
}

// TestFlushPendingSubIndex is the multi-flush-per-coordinate dedup-loss regression at
// the flush boundary: partial flushes that share one binlog coordinate (an
// oversized RowsEvent stamps every flush with the event's end offset) must get
// strictly-increasing SourceSeqs, or the ingest dedup's <= drop silently loses
// every flush after the first. A flush at a new coordinate resets the sub-index.
func TestFlushPendingSubIndex(t *testing.T) {
	ch := make(chan *cluster.Proposal, 8)
	h := &MySQLEventHandler{proposalChan: ch, curFile: "binlog.000007", curPos: 900}

	flush := func() uint64 {
		h.pending = []*cluster.Entity{{Key: []byte("k")}}
		require.NoError(t, h.flushPending(context.Background(), false))
		return (<-ch).SourceSeq
	}

	// Three flushes at the same coordinate → strictly increasing seqs.
	s0, s1, s2 := flush(), flush(), flush()
	require.True(t, s0 < s1 && s1 < s2, "same-coordinate flushes must strictly increase: %d, %d, %d", s0, s1, s2)
	require.Equal(t, encodeSourceSeq("binlog.000007", 900, 0), s0, "the first flush is the bare coordinate")

	// Advancing the coordinate resets the sub-index (back to the bare value) while
	// still advancing overall.
	h.curPos = 950
	s3 := flush()
	require.Equal(t, encodeSourceSeq("binlog.000007", 950, 0), s3, "a new coordinate resets the sub-index")
	require.Greater(t, s3, s2, "and still advances overall")
}
