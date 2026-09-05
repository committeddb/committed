package http

import (
	"encoding/json"
	httpgo "net/http"

	"github.com/committeddb/committed/internal/cluster"
)

// NodeStatusResponse is the body of GET /node/status — per-node
// diagnostics for the node that served the request. The fields are
// node-local and ephemeral (not part of the replicated config content),
// so two nodes can legitimately return different bodies for the same
// cluster state. node is the answering node's raft ID — load-bearing
// behind a load balancer, where the path scope ("this node") doesn't tell
// you which node "this" is. leader and appliedIndex mirror the raft
// details the unauthenticated /ready probe deliberately omits, making
// /node/status their queryable, authenticated counterpart.
type NodeStatusResponse struct {
	Node            uint64                   `json:"node"`
	Leader          uint64                   `json:"leader"`
	AppliedIndex    uint64                   `json:"appliedIndex"`
	DegradedConfigs []DegradedConfigResponse `json:"degradedConfigs"`
	Disk            DiskStatusResponse       `json:"disk"`
	// SafeMode reports whether this node booted with COMMITTED_SAFE_MODE:
	// sync/ingest/scrub workers held, raft/apply/API normal. Per-boot and
	// node-local — the operator's confirmation that the escape hatch is
	// active (vs. workers being mysteriously absent). See
	// docs/operations/safe-mode.md.
	SafeMode bool `json:"safeMode"`
	// Scrub is this node's right-to-be-forgotten scrub progress; erasure
	// through PendingBound is physically complete on THIS node once
	// completedBound catches up to pendingBound (each node rewrites its own
	// log — poll every node). pendingDeleteKeyErasures counts retained
	// delete tombstones whose raw subject key is not yet erased — zero is
	// the identifier-erasure end condition.
	Scrub ScrubStatusResponse `json:"scrub"`
}

// DiskStatusResponse reports this node's disk pressure and the
// write-admission decision its propose gate is applying. state is the
// node-local watcher level; admission is the cluster-aware verdict (or the
// node-local fallback when no fresh verdict is held). This is the queryable
// "writable" signal — deliberately separate from /ready, which feeds load
// balancers and would also drain the reads a low-disk node can still serve.
type DiskStatusResponse struct {
	State     string                `json:"state"`
	Admission DiskAdmissionResponse `json:"admission"`
}

// DiskAdmissionResponse is one node's current write-admission view. admitted
// says whether user-data writes are accepted at this node right now; state
// and reason are the disk level and cause driving that decision; source is
// "cluster" when a fresh leader-computed verdict is in force or "local" when
// the gate has fallen back to the node-local Phase 1 decision; leader is the
// verdict's computing leader (omitted under "local").
type DiskAdmissionResponse struct {
	Admitted bool   `json:"admitted"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	Source   string `json:"source"`
	Leader   uint64 `json:"leader,omitempty"`
}

// DegradedConfigResponse names one config this node persisted but could
// not build into a live object — a node-local condition (usually a
// missing ${VAR} secret on this node), not a defect in the replicated
// bytes, which are valid cluster-wide. error names the failing ${VAR},
// never an interpolated value (interpolation failed, so none exists).
type DegradedConfigResponse struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	Error string `json:"error"`
}

// ScrubStatusResponse is the node-local scrub block of /node/status.
type ScrubStatusResponse struct {
	PendingBound             uint64 `json:"pendingBound"`
	CompletedBound           uint64 `json:"completedBound"`
	PendingDeleteKeyErasures int    `json:"pendingDeleteKeyErasures"`
}

// NodeStatus serves GET /node/status: this node's degraded configs plus a
// little raft identity. It is authenticated (same group as the config
// endpoints) and answers for the node that received the request — the
// queryable, authenticated diagnosis behind the committed_config_build_errors
// gauge, which can alert "node N has a degraded config" but can't say which
// or why. A healthy node returns an empty degradedConfigs array.
func (h *HTTP) NodeStatus(w httpgo.ResponseWriter, r *httpgo.Request) {
	degraded := degradedConfigsResponse(h.db.ConfigBuildErrors())

	admission := h.db.DiskAdmission()
	resp := NodeStatusResponse{
		Node:            h.db.ID(),
		Leader:          h.db.Leader(),
		AppliedIndex:    h.db.AppliedIndex(),
		DegradedConfigs: degraded,
		SafeMode:        h.db.SafeMode(),
		Scrub: func() ScrubStatusResponse {
			s := h.db.ScrubStatus()
			return ScrubStatusResponse{
				PendingBound:             s.PendingBound,
				CompletedBound:           s.CompletedBound,
				PendingDeleteKeyErasures: s.PendingDeleteKeyErasures,
			}
		}(),
		Disk: diskStatusResponse(h.db.DiskState(), admission),
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// degradedConfigsResponse renders the node-local build failures. The empty
// slice (never nil) is load-bearing: a JSON null would force every client to
// special-case it. A free function so the rendering is testable directly —
// inducing a real build failure needs an engine restart with a ${VAR}
// removed, which the status tests do not pay for.
func degradedConfigsResponse(errs []cluster.ConfigBuildError) []DegradedConfigResponse {
	degraded := make([]DegradedConfigResponse, 0, len(errs))
	for _, e := range errs {
		degraded = append(degraded, DegradedConfigResponse{Kind: e.Kind, ID: e.ID, Error: e.Error})
	}
	return degraded
}

// diskStatusResponse renders the node's disk block: the LOCAL disk level
// next to the admission decision the gate is applying (which can
// legitimately diverge — a cluster verdict can admit while the local disk
// reads full, and vice versa). A free function so both shapes are testable
// directly — driving the real disk watcher through its levels means
// manipulating real disk-usage thresholds, which the status tests do not
// pay for.
func diskStatusResponse(state string, admission cluster.DiskAdmissionStatus) DiskStatusResponse {
	return DiskStatusResponse{
		State: state,
		Admission: DiskAdmissionResponse{
			Admitted: admission.Admitted,
			State:    admission.State,
			Reason:   admission.Reason,
			Source:   admission.Source,
			Leader:   admission.LeaderID,
		},
	}
}
