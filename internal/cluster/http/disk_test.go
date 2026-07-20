package http_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

func doDiskReport(t *testing.T, h *http.HTTP, body string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest("POST", "http://localhost/v1/node/disk-report", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, bs
}

// TestDiskReport_ReturnsVerdict verifies the happy path: the report is
// handed to the cluster (node id + state intact) and the verdict comes back
// in the wire shape the db-side sender decodes.
func TestDiskReport_ReturnsVerdict(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ReportDiskReturns(cluster.DiskVerdict{
		State:    "critical",
		Reason:   "quorum at risk: 1 of 3 voters have disk headroom (need 2)",
		LeaderID: 1,
	}, nil)
	h := http.New(fake)

	status, bs := doDiskReport(t, h, `{"node":2,"state":"full"}`)

	require.Equal(t, 200, status)
	var body http.DiskReportResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	require.Equal(t, "critical", body.State)
	require.Contains(t, body.Reason, "quorum at risk")
	require.Equal(t, uint64(1), body.Leader)

	require.Equal(t, 1, fake.ReportDiskCallCount())
	gotNode, gotState := fake.ReportDiskArgsForCall(0)
	require.Equal(t, uint64(2), gotNode)
	require.Equal(t, "full", gotState)
}

// TestDiskReport_NotLeader verifies a report landing on a non-leader gets
// 503 leader_unavailable with the believed leader id in the details — the
// signal for the reporter to re-resolve the leader on its next cycle.
func TestDiskReport_NotLeader(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ReportDiskReturns(cluster.DiskVerdict{LeaderID: 3}, cluster.ErrNotLeader)
	h := http.New(fake)

	status, bs := doDiskReport(t, h, `{"node":2,"state":"ok"}`)

	require.Equal(t, 503, status)
	require.Contains(t, string(bs), "leader_unavailable")
	require.Contains(t, string(bs), `"leaderId":3`)
}

// TestDiskReport_BadRequests verifies the 400 paths: malformed JSON, a
// missing node id, and a state the cluster rejects as unknown.
func TestDiskReport_BadRequests(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ReportDiskReturns(cluster.DiskVerdict{}, errors.New(`unknown disk state "toasty"`))
	h := http.New(fake)

	status, bs := doDiskReport(t, h, `{not json`)
	require.Equal(t, 400, status)
	require.Contains(t, string(bs), "invalid_json")

	status, bs = doDiskReport(t, h, `{"state":"ok"}`)
	require.Equal(t, 400, status)
	require.Contains(t, string(bs), "invalid_disk_report")
	require.Zero(t, fake.ReportDiskCallCount(), "a report without a node id must not reach the cluster")

	status, bs = doDiskReport(t, h, `{"node":2,"state":"toasty"}`)
	require.Equal(t, 400, status)
	require.Contains(t, string(bs), "invalid_disk_report")
}

// TestNodeStatus_DiskAdmission verifies GET /node/status carries the local
// disk level and the admission decision the gate is applying, in both the
// cluster-verdict and local-fallback shapes.
func TestNodeStatus_DiskAdmission(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(1)
	fake.AppliedIndexReturns(42)
	fake.DiskStateReturns("full")
	fake.DiskAdmissionReturns(cluster.DiskAdmissionStatus{
		Admitted: true,
		State:    "ok",
		Source:   "cluster",
		LeaderID: 1,
	})
	h := http.New(fake)

	status, body := doNodeStatus(t, h)

	require.Equal(t, 200, status)
	require.Equal(t, "full", body.Disk.State, "local state and admission state can legitimately diverge")
	require.True(t, body.Disk.Admission.Admitted)
	require.Equal(t, "ok", body.Disk.Admission.State)
	require.Equal(t, "cluster", body.Disk.Admission.Source)
	require.Equal(t, uint64(1), body.Disk.Admission.Leader)

	fake.DiskAdmissionReturns(cluster.DiskAdmissionStatus{
		Admitted: false,
		State:    "full",
		Reason:   "node-local disk full",
		Source:   "local",
	})
	status, body = doNodeStatus(t, h)
	require.Equal(t, 200, status)
	require.False(t, body.Disk.Admission.Admitted)
	require.Equal(t, "local", body.Disk.Admission.Source)
	require.Equal(t, "node-local disk full", body.Disk.Admission.Reason)
	require.Zero(t, body.Disk.Admission.Leader)
}
