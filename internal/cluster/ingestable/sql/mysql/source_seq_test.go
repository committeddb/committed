package mysql

import "testing"

// TestEncodeSourceSeq verifies the (file, offset) -> uint64 mapping is
// strictly monotonic in binlog order and degrades safely to 0 on an
// unparseable file name (which disables dedup for that proposal rather
// than risking a false-positive skip).
func TestEncodeSourceSeq(t *testing.T) {
	// Within a file, a larger offset is a larger seq.
	if a, b := encodeSourceSeq("binlog.000042", 100), encodeSourceSeq("binlog.000042", 200); a >= b {
		t.Fatalf("within-file: encode(.42,100)=%d should be < encode(.42,200)=%d", a, b)
	}

	// A later file outranks an earlier one regardless of offset — the
	// rotation case (earlier file at a huge offset vs new file at offset 4).
	if a, b := encodeSourceSeq("binlog.000041", 1<<31), encodeSourceSeq("binlog.000042", 4); a >= b {
		t.Fatalf("rotation: encode(.41,big)=%d should be < encode(.42,4)=%d", a, b)
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
		got := encodeSourceSeq(c.file, c.pos)
		if got <= prev {
			t.Fatalf("non-monotonic at %s:%d -> %d (prev %d)", c.file, c.pos, got, prev)
		}
		prev = got
	}

	// Unparseable file names map to 0 (dedup disabled, never a false skip).
	for _, bad := range []string{"", "noNumber", "binlog.", "binlog.abc", "trailingdot."} {
		if got := encodeSourceSeq(bad, 123); got != 0 {
			t.Fatalf("encode(%q,123) = %d, want 0", bad, got)
		}
	}
}
