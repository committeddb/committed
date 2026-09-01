package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Rendering and mapping tables for the status group — the rows a real
// single-node engine cannot induce cheaply (a parked worker means a tripped
// circuit breaker, a degraded config means a restart with a ${VAR} removed,
// not-leader means a second node, and divergent disk shapes mean driving the
// real disk watcher through its levels). The inducible rows run end to end
// in status_test.go / cluster_status_test.go / disk_test.go / scrub_test.go.

// TestDegradedConfigsResponse pins the degraded-config rendering: kind, id,
// and the (already redacted) error pass through, and the empty case is an
// empty slice, never nil — a JSON null would force every client to
// special-case it.
func TestDegradedConfigsResponse(t *testing.T) {
	out := degradedConfigsResponse([]cluster.ConfigBuildError{
		{Kind: "database", ID: "orders-warehouse", Error: "missing environment variable ${WAREHOUSE_PW}"},
	})
	require.Equal(t, []DegradedConfigResponse{
		{Kind: "database", ID: "orders-warehouse", Error: "missing environment variable ${WAREHOUSE_PW}"},
	}, out)

	require.NotNil(t, degradedConfigsResponse(nil), "empty must be [] not null")
	require.Empty(t, degradedConfigsResponse(nil))
}

// TestParkedWorkersResponse pins the parked-worker rendering (sync and
// ingest kinds) and the []-not-null guarantee.
func TestParkedWorkersResponse(t *testing.T) {
	out := parkedWorkersResponse([]cluster.ParkedWorker{
		{Kind: "sync", ID: "orders-sync"},
		{Kind: "ingest", ID: "catalog-ingest"},
	})
	require.Equal(t, []ParkedWorkerResponse{
		{Kind: "sync", ID: "orders-sync"},
		{Kind: "ingest", ID: "catalog-ingest"},
	}, out)

	require.NotNil(t, parkedWorkersResponse(nil), "empty must be [] not null")
	require.Empty(t, parkedWorkersResponse(nil))
}

// TestDiskStatusResponse pins both admission shapes: the cluster verdict
// (local state and admission state legitimately diverging) and the
// node-local fallback.
func TestDiskStatusResponse(t *testing.T) {
	got := diskStatusResponse("full", cluster.DiskAdmissionStatus{
		Admitted: true, State: "ok", Source: "cluster", LeaderID: 1,
	})
	require.Equal(t, "full", got.State, "local state and admission state can legitimately diverge")
	require.True(t, got.Admission.Admitted)
	require.Equal(t, "ok", got.Admission.State)
	require.Equal(t, "cluster", got.Admission.Source)
	require.Equal(t, uint64(1), got.Admission.Leader)

	got = diskStatusResponse("full", cluster.DiskAdmissionStatus{
		Admitted: false, State: "full", Reason: "node-local disk full", Source: "local",
	})
	require.False(t, got.Admission.Admitted)
	require.Equal(t, "local", got.Admission.Source)
	require.Equal(t, "node-local disk full", got.Admission.Reason)
	require.Zero(t, got.Admission.Leader)
}

// TestWriteDiskReportError pins the not-leader leg (a single-node engine is
// always the leader): 503 leader_unavailable with the believed leader id in
// the details, the reporter's signal to re-resolve — and the default 400.
func TestWriteDiskReportError(t *testing.T) {
	w := httptest.NewRecorder()
	writeDiskReportError(w, cluster.DiskVerdict{LeaderID: 3}, cluster.ErrNotLeader)
	require.Equal(t, 503, w.Code)
	require.Contains(t, w.Body.String(), "leader_unavailable")
	require.Contains(t, w.Body.String(), `"leaderId":3`)

	w = httptest.NewRecorder()
	writeDiskReportError(w, cluster.DiskVerdict{}, fmt.Errorf(`unknown disk state "toasty"`))
	require.Equal(t, 400, w.Code)
	require.Contains(t, w.Body.String(), "invalid_disk_report")
}

// TestWriteProposeError_Mapping pins the full branch table of the shared
// propose-error choke point every config write, the scrub lever, and the
// rollback path route through. Route-level coverage exercises the common
// rows per resource; this is the one place the whole matrix lives.
func TestWriteProposeError_Mapping(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"below feature level is 503", &cluster.ClusterBelowFeatureLevelError{Feature: "x", Required: 3, ClusterMin: 1}, 503, "cluster_below_feature_level"},
		{"stranded consumers is 409", &cluster.StrandedSyncablesError{TypeID: "t", Version: 2, Syncables: []string{"s1"}}, 409, "stranded_always_current"},
		{"config error is 400", &cluster.ConfigError{Err: fmt.Errorf("bad toml")}, 400, "invalid_widget_config"},
		{"proposal too large is 413", fmt.Errorf("wrap: %w", cluster.ErrProposalTooLarge), 413, "proposal_too_large"},
		{"disk full is 507", fmt.Errorf("wrap: %w", cluster.ErrInsufficientStorage), 507, "insufficient_storage"},
		{"deadline is 503", context.DeadlineExceeded, 503, "request_unconfirmed"},
		{"cancellation is 503", context.Canceled, 503, "request_unconfirmed"},
		{"unconfirmed is 503", fmt.Errorf("wrap: %w", cluster.ErrProposalUnconfirmed), 503, "request_unconfirmed"},
		{"unknown failure is 500", io.ErrUnexpectedEOF, 500, "internal_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeProposeError(w, tc.err, "widget", "frob widget")
			var body struct {
				Code string `json:"code"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, tc.wantStatus, w.Code)
			require.Equal(t, tc.wantCode, body.Code)
		})
	}
}
