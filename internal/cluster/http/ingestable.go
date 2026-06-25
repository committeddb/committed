package http

import (
	"encoding/json"
	"errors"
	httpgo "net/http"

	"github.com/committeddb/committed/internal/cluster"
)

// IngestableStatusResponse is the operational status of an ingestable's worker:
// which phase it is in, how far the initial snapshot got, where the CDC cursor
// sits, how far behind the source it is, and whether it is fully caught up. It
// is the ingest analogue of SyncableStatusResponse.
type IngestableStatusResponse struct {
	// Phase is "snapshot" while dumping existing rows, then "streaming" once on
	// the change-data-capture stream.
	Phase string `json:"phase"`
	// SnapshotProgress is per watched table — present in both phases (every
	// table reads complete once the snapshot finishes).
	SnapshotProgress []TableSnapshotProgress `json:"snapshotProgress"`
	// Position is the dialect's CDC cursor in native text form: a Postgres LSN
	// ("0/1A2B3C8") or a MySQL binlog coordinate ("binlog.000004:1547").
	Position string `json:"position"`
	// Lag is bytes the source write head is ahead of what this ingest has
	// durably consumed. null while snapshotting, on MySQL (lag is a follow-on),
	// or when the source is unreachable; a non-null 0 means fully caught up.
	Lag *uint64 `json:"lag"`
	// CaughtUp is true exactly when the snapshot is complete and lag is a known
	// 0 — never true while lag is null.
	CaughtUp bool `json:"caughtUp"`
}

// TableSnapshotProgress is one watched table's place in the initial snapshot.
type TableSnapshotProgress struct {
	Table string `json:"table"`
	// LastKey is the keyset cursor reached; omitted once the table is complete
	// (the cursor is no longer tracked) or before its snapshot starts.
	LastKey  string `json:"lastKey,omitempty"`
	Complete bool   `json:"complete"`
}

// GetIngestableStatus reports an ingestable worker's operational status
// (GET /ingestable/{id}/status): snapshot vs. streaming phase, per-table
// snapshot progress, the CDC position, source lag, and whether it is caught up.
// It is how an operator (or a dashboard) answers "is the snapshot done? where's
// the CDC cursor? is it caught up? how far behind?" without grepping logs —
// the ingest analogue of GET /syncable/{id}/status. Read behind the same
// linearize barrier as the other status reads, so the persisted position is
// consistent on whichever node answers; the lag number is a live source query.
func (h *HTTP) GetIngestableStatus(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	if !h.linearize(w, r) {
		return
	}

	st, err := h.c.IngestableStatus(r.Context(), id)
	if errors.Is(err, cluster.ErrIngestableNotRunning) {
		writeError(w, httpgo.StatusNotFound, "ingestable_not_running",
			"no ingestable worker is running for this id on the node that answered")
		return
	}
	if err != nil {
		writeInternalError(w, "failed to retrieve ingestable status", err)
		return
	}

	resp := toIngestableStatusResponse(st)

	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// toIngestableStatusResponse converts the cluster-level ingestable status into
// the HTTP response shape. Shared by GetIngestableStatus and the pipeline view.
func toIngestableStatusResponse(st cluster.IngestableStatus) IngestableStatusResponse {
	resp := IngestableStatusResponse{
		Phase:            st.Phase,
		SnapshotProgress: make([]TableSnapshotProgress, 0, len(st.SnapshotProgress)),
		Position:         st.Position,
		Lag:              st.Lag,
		CaughtUp:         st.CaughtUp,
	}
	for _, t := range st.SnapshotProgress {
		resp.SnapshotProgress = append(resp.SnapshotProgress, TableSnapshotProgress{
			Table:    t.Table,
			LastKey:  t.LastKey,
			Complete: t.Complete,
		})
	}
	return resp
}
