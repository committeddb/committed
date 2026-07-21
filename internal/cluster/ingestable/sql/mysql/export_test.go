package mysql

import (
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// SetTxnSoftFlushBytesForTest lowers the per-flush byte budget so a test can
// force several partial flushes out of one small RowsEvent (instead of needing a
// giant event), and returns a restore func. Test-only: export_test.go is compiled
// solely for tests, so this exposes the shared knob without widening the API.
func SetTxnSoftFlushBytesForTest(n int) func() {
	orig := sql.TxnSoftFlushBytes
	sql.TxnSoftFlushBytes = n
	return func() { sql.TxnSoftFlushBytes = orig }
}
