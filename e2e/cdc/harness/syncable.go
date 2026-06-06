//go:build docker

package harness

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/committeddb/committed/e2e/cdc/dataset"
)

// SinkTable is the table a topic's syncable projects into. It must differ
// from the CDC source table (same Postgres instance) so the syncable's writes
// aren't themselves captured — only public.<table> is in the publication, so
// <table>_sink writes never loop back.
func SinkTable(topic string) string { return topic + "_sink" }

// sinkDatabaseID is the id (and TOML name) of the single sink database config
// every syncable references via `db = "..."`. Database configs are keyed by
// their URL id, which is also what sql.db resolves against.
const sinkDatabaseID = "sink"

// postSinkDatabase registers one database config pointing back at the
// harness's own Postgres, using the postgres syncable dialect. This is the
// dialect cmd/node.go's dbParser only started registering once the
// postgres-syncable-dialect bug was fixed.
func postSinkDatabase(t *testing.T, connStr string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[database]\nname = %q\ntype = \"sql\"\n\n", sinkDatabaseID)
	fmt.Fprintf(&b, "[sql]\ndialect = \"postgres\"\n")
	fmt.Fprintf(&b, "connectionString = %q\n", connStr)
	postConfig(t, "/v1/database/"+sinkDatabaseID, b.String())
}

// postSyncable registers a syncable that projects one topic into its
// <table>_sink table via the sink database. Every column is mapped as TEXT
// because pgoutput text-encodes CDC values, so the topic JSON carries strings
// (e.g. r_regionkey arrives as "1", not 1).
func postSyncable(t *testing.T, table string) {
	t.Helper()
	sink := SinkTable(table)
	pk := dataset.PrimaryKey(table)

	var b strings.Builder
	fmt.Fprintf(&b, "[syncable]\nname = %q\ntype = \"sql\"\n\n", table)
	fmt.Fprintf(&b, "[sql]\ntopic = %q\ndb = %q\ntable = %q\nprimaryKey = %q\n\n",
		table, sinkDatabaseID, sink, pk)
	for _, col := range dataset.Columns(table) {
		fmt.Fprintf(&b, "[[sql.mappings]]\njsonPath = \"$.%s\"\ncolumn = %q\ntype = \"TEXT\"\n\n",
			col, col)
	}
	postConfig(t, "/v1/syncable/"+table, b.String())
}

// SinkValue returns the value of column `col` for the row whose primary key
// equals `pk` in a topic's sink table, and whether such a row exists. A
// missing table or row (the syncable creates the table lazily and writes
// asynchronously) reports (", false) rather than failing — callers poll via
// WaitForSinkValue.
func (h *Harness) SinkValue(table, pk, col string) (string, bool) {
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		col, SinkTable(table), dataset.PrimaryKey(table))
	var v string
	if err := h.pgConn.QueryRow(h.ctx, q, pk).Scan(&v); err != nil {
		return "", false
	}
	return v, true
}

// WaitForSinkValue polls until the sink row pk has col == want, or fails the
// test after timeout. This is the syncable analogue of waitForCounts: it is
// how a test asserts that committed projected a proposal all the way out to
// the external database.
func (h *Harness) WaitForSinkValue(t *testing.T, table, pk, col, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	var seen bool
	for time.Now().Before(deadline) {
		if v, ok := h.SinkValue(table, pk, col); ok {
			last, seen = v, true
			if v == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("sink %s: no row with %s=%q within %s (syncable never wrote it)",
			SinkTable(table), dataset.PrimaryKey(table), pk, timeout)
	}
	t.Fatalf("sink %s: row %s=%q has %s=%q, wanted %q",
		SinkTable(table), dataset.PrimaryKey(table), pk, col, last, want)
}

// WaitForSinkAbsent polls until the sink row pk no longer exists, or fails
// the test after timeout. This is how a test asserts a delete was honored all
// the way out to the external database: the syncable translated a delete
// Actual into a DELETE that removed the row. Callers should first confirm the
// row was present (WaitForSinkValue) so that absence proves removal, not a
// row that never arrived.
func (h *Harness) WaitForSinkAbsent(t *testing.T, table, pk string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := h.SinkValue(table, pk, dataset.PrimaryKey(table)); !ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("sink %s: row %s=%q still present after %s (delete was not honored)",
		SinkTable(table), dataset.PrimaryKey(table), pk, timeout)
}

// SinkCount returns the number of rows in a topic's sink table, or 0 if the
// table does not exist yet.
func (h *Harness) SinkCount(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := h.pgConn.QueryRow(h.ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", SinkTable(table))).Scan(&n); err != nil {
		return 0
	}
	return n
}
