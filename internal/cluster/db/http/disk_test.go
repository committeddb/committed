package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/http"
)

func doDiskReport(t *testing.T, e *engine, body string) (int, []byte) {
	t.Helper()
	w := e.doJSON(t, "POST", "/v1/node/disk-report", body)
	return w.Code, w.Body.Bytes()
}

// TestDiskReport_ReturnsVerdict: the happy path against the real engine —
// the single node IS the leader, aggregates its own report, and answers in
// the wire shape the db-side sender decodes. (The not-leader 503 leg cannot
// be induced on a single node; its mapping is pinned in
// status_mapping_test.go.)
func TestDiskReport_ReturnsVerdict(t *testing.T) {
	e := newEngine(t)

	status, bs := doDiskReport(t, e, `{"node":1,"state":"ok"}`)

	require.Equal(t, 200, status, string(bs))
	var body http.DiskReportResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	require.Equal(t, "ok", body.State)
	require.Equal(t, uint64(1), body.Leader)
}

// TestDiskReport_BadRequests: malformed JSON, a missing node id, and a state
// the engine rejects as unknown are all 400s.
func TestDiskReport_BadRequests(t *testing.T) {
	e := newEngine(t)

	status, bs := doDiskReport(t, e, `{not json`)
	require.Equal(t, 400, status)
	require.Contains(t, string(bs), "invalid_json")

	status, bs = doDiskReport(t, e, `{"state":"ok"}`)
	require.Equal(t, 400, status)
	require.Contains(t, string(bs), "invalid_disk_report")

	status, bs = doDiskReport(t, e, `{"node":2,"state":"toasty"}`)
	require.Equal(t, 400, status, "the real engine must reject an unknown disk state")
	require.Contains(t, string(bs), "invalid_disk_report")
}
