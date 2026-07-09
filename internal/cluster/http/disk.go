package http

import (
	"encoding/json"
	"errors"
	httpgo "net/http"

	"github.com/committeddb/committed/internal/cluster"
)

// DiskReportRequest is the body of POST /v1/node/disk-report: one member's
// node-local disk-pressure level, pushed to the leader on the disk
// coordinator's cadence. node is the reporting member's raft ID and state is
// its watcher level ("ok", "warn", "critical", or "full"). The wire shape is
// mirrored by the db-side sender (diskReportWire in db/disk_cluster.go).
type DiskReportRequest struct {
	Node  uint64 `json:"node"`
	State string `json:"state"`
}

// DiskReportResponse is the verdict piggybacked on a disk report: the
// cluster-effective disk state the leader computed from all voter states,
// the operator-facing reason when it is degraded, and the computing leader's
// raft ID. The reporting member caches it and enforces it at its own propose
// gate. Mirrored by diskVerdictWire in db/disk_cluster.go.
type DiskReportResponse struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
	Leader uint64 `json:"leader"`
}

// DiskReport serves POST /v1/node/disk-report — the node-to-node half of
// cluster-aware disk admission. Members push their disk state here (to the
// leader's announced API URL, authenticated with the cluster's bearer token
// like every other write) and take the admission verdict home from the
// response, so one round trip per member per interval keeps the leader's
// quorum math and every member's gate fresh.
//
// Only the leader aggregates reports: when leadership has moved since the
// reporter resolved this node's URL, it answers 503 leader_unavailable with
// the believed leader id, and the reporter re-resolves on its next cycle.
func (h *HTTP) DiskReport(w httpgo.ResponseWriter, r *httpgo.Request) {
	req := &DiskReportRequest{}
	if err := unmarshalBody(r, req); err != nil {
		h.writeReadError(w, r, err, "invalid_json", "request body is not valid JSON")
		return
	}
	if req.Node == 0 {
		writeError(w, httpgo.StatusBadRequest, "invalid_disk_report", "node must be a non-zero raft ID")
		return
	}

	verdict, err := h.c.ReportDisk(req.Node, req.State)
	if err != nil {
		if errors.Is(err, cluster.ErrNotLeader) {
			writeLeaderUnavailable(w, verdict.LeaderID,
				"disk reports are aggregated on the leader; re-resolve the leader and retry")
			return
		}
		writeError(w, httpgo.StatusBadRequest, "invalid_disk_report", err.Error())
		return
	}

	bs, err := json.Marshal(DiskReportResponse{
		State:  verdict.State,
		Reason: verdict.Reason,
		Leader: verdict.LeaderID,
	})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}
