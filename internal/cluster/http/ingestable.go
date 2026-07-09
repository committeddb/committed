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
	// Lag is how far the source write head is ahead of what this ingest has
	// durably consumed, in the dialect's natural unit: Postgres bytes, MySQL
	// (GTID) transactions. null while snapshotting, when the source is
	// unreachable, on a MySQL source without GTID positioning, or when a
	// re-snapshot is required; a non-null 0 means fully caught up.
	Lag *uint64 `json:"lag"`
	// CaughtUp is true exactly when the snapshot is complete and lag is a known
	// 0 — never true while lag is null.
	CaughtUp bool `json:"caughtUp"`
	// ReSnapshotRequired is true when the source discarded change data this
	// ingest never consumed (MySQL: binlogs purged past the consumed GTID set) —
	// a distinct state, not a lag number. Recovery is a fresh snapshot. Always
	// false for Postgres.
	ReSnapshotRequired bool `json:"reSnapshotRequired"`
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

// IngestableDeleteResponse confirms an ingestable was deleted.
type IngestableDeleteResponse struct {
	ID string `json:"id"`
}

// DeleteIngestable (DELETE /ingestable/{id}) removes an ingestable: its config
// and checkpoint position are deleted atomically (consensus), the worker is
// stopped, and the owner drops the source-side replication resources (the
// Postgres slot + publication) best-effort — an orphaned slot would otherwise
// pin the source's WAL and fill its disk. A later same-id POST starts fresh from
// a full snapshot.
//
// The logical deletion is authoritative and completes before this returns
// (Propose blocks until the Actual applies); the source teardown is a best-effort
// side effect that settles shortly after on the leader. The route is leader-
// pinned so this runs where the teardown does.
func (h *HTTP) DeleteIngestable(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	if err := h.c.DeleteIngestable(r.Context(), id); err != nil {
		writeProposeError(w, err, "ingestable", "delete ingestable")
		return
	}

	bs, err := json.Marshal(IngestableDeleteResponse{ID: id})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpgo.StatusOK)
	_, _ = w.Write(bs)
}

// toIngestableStatusResponse converts the cluster-level ingestable status into
// the HTTP response shape. Shared by GetIngestableStatus and the pipeline view.
func toIngestableStatusResponse(st cluster.IngestableStatus) IngestableStatusResponse {
	resp := IngestableStatusResponse{
		Phase:              st.Phase,
		SnapshotProgress:   make([]TableSnapshotProgress, 0, len(st.SnapshotProgress)),
		Position:           st.Position,
		Lag:                st.Lag,
		CaughtUp:           st.CaughtUp,
		ReSnapshotRequired: st.ReSnapshotRequired,
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
