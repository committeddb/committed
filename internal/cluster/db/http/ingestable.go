package http

import (
	"encoding/json"
	"errors"
	httpgo "net/http"
	"sort"

	"github.com/committeddb/committed/internal/cluster"
)

// IngestableStatusResponse is the operational status of an ingestable's worker:
// which phase it is in, how far the initial snapshot got, where the CDC cursor
// sits, how far behind the source it is, and whether it is fully caught up. It
// is the ingest analogue of SyncableStatusResponse.
type IngestableStatusResponse struct {
	// WorkerState is the worker's lifecycle state: "running" or "parked" (the
	// freeze/restart supervisor gave up — fix the config and re-POST it, or delete).
	// Replicated, so it is reported truthfully from any node.
	WorkerState string `json:"workerState"`
	// Phase is "pending" until anything has durably checkpointed, "snapshot"
	// while dumping existing rows, then "streaming" once on the
	// change-data-capture stream. Omitted when workerState is "parked" (a
	// stopped worker has no phase).
	Phase string `json:"phase,omitempty"`
	// SnapshotProgress is per watched table — present in both phases (every
	// table reads complete once the snapshot finishes).
	SnapshotProgress []TableSnapshotProgress `json:"snapshotProgress"`
	// Position is the dialect's CDC cursor in native text form: a Postgres LSN
	// ("0/1A2B3C8") or a MySQL binlog coordinate ("binlog.000004:1547").
	Position string `json:"position"`
	// Lag is how far the source write head is ahead of what this ingest has
	// durably consumed, in the unit lagUnit names: Postgres bytes, MySQL
	// transactions under GTID positioning, MySQL bytes under file:pos
	// positioning. null while snapshotting, when the source is unreachable,
	// or when a re-snapshot is required; a non-null 0 means fully caught up.
	Lag *uint64 `json:"lag"`
	// LagUnit is "bytes" or "transactions" — which scale Lag is on. Omitted
	// exactly when lag is null.
	LagUnit string `json:"lagUnit,omitempty"`
	// CaughtUp is true exactly when the snapshot is complete and lag is a known
	// 0 — never true while lag is null.
	CaughtUp bool `json:"caughtUp"`
	// ReSnapshotRequired is true when the source discarded change data this
	// ingest never consumed (MySQL: binlogs purged past the consumed GTID set) —
	// a distinct state, not a lag number. Recovery is a fresh snapshot. Always
	// false for Postgres.
	ReSnapshotRequired bool `json:"reSnapshotRequired"`
	// Census is the JSON shape census the worker took during its snapshot
	// pass, per topic — the type-drafting bootstrap. Omitted until a census
	// has been published (census opted out, or no snapshot yet). Types and
	// paths only; the draft schema carries enum values only when the
	// ingestable opted into censusValues.
	Census map[string]*TopicCensusResponse `json:"census,omitempty"`
}

// TopicCensusResponse is one topic's shape census: the distinct payload
// shapes with their row ranges (the interleaved-shapes evidence), the derived
// per-path view, and a DRAFT JSON Schema for POST /type review — inference is
// bootstrap-only, the draft is never auto-blessed.
type TopicCensusResponse struct {
	RefreshEpoch uint64                `json:"refreshEpoch"`
	Rows         uint64                `json:"rows"`
	Shapes       []ShapeCensusResponse `json:"shapes"`
	Paths        []*cluster.PathCensus `json:"paths"`
	DraftSchema  string                `json:"draftSchema,omitempty"`
}

// ShapeCensusResponse is one distinct payload shape observed on the topic.
type ShapeCensusResponse struct {
	Fingerprint string   `json:"fingerprint"`
	Shape       []string `json:"shape"`
	Count       uint64   `json:"count"`
	FirstRow    uint64   `json:"firstRow"`
	LastRow     uint64   `json:"lastRow"`
}

// TableSnapshotProgress is one watched table's place in the initial snapshot.
// The keyset cursor (last PK dumped) is intentionally not exposed — a natural PK
// is often source PII; Complete answers "is this table done" without it.
type TableSnapshotProgress struct {
	Table    string `json:"table"`
	Topic    string `json:"topic"`
	Complete bool   `json:"complete"`
	// ChunksTotal/ChunksDone report a chunked parallel snapshot's progress
	// (snapshot_readers > 1 with a splittable PK). Omitted for a
	// single-stream table.
	ChunksTotal int `json:"chunksTotal,omitempty"`
	ChunksDone  int `json:"chunksDone,omitempty"`
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

	// Split the two absences: an id that does not EXIST 404s as not_found
	// (a typo'd id must not read as "exists, but no worker here" — the
	// syncable-status phantom's sibling), while a real config with no local
	// worker keeps the ingestable_not_running 404 below.
	if ok, err := h.c.IngestableExists(id); err != nil {
		writeInternalError(w, "failed to check ingestable existence", err)
		return
	} else if !ok {
		writeError(w, httpgo.StatusNotFound, "not_found", "ingestable not found")
		return
	}

	st, err := h.c.IngestableStatus(r.Context(), id)
	if errors.Is(err, cluster.ErrIngestableNotRunning) {
		// When the answering node's degraded-config record explains the
		// absence (the config persisted but its node-local build failed —
		// e.g. a missing ${VAR}), say so: the generic message would send
		// the operator hunting through the node-status surface for a cause
		// this handler already knows. The error is redacted at the record
		// surface. Mirrors the syncable status's workerState "degraded".
		msg := "no ingestable worker is running for this id on the node that answered"
		for _, ce := range h.c.ConfigBuildErrors() {
			if ce.Kind == "ingestable" && ce.ID == id {
				msg = "the config failed to build on the node that answered (no worker started): " + ce.Error
				break
			}
		}
		writeError(w, httpgo.StatusNotFound, "ingestable_not_running", msg)
		return
	}
	if err != nil {
		writeInternalError(w, "failed to retrieve ingestable status", err)
		return
	}

	resp := toIngestableStatusResponse(st)
	if census, ok := h.c.IngestableCensus(id); ok {
		resp.Census = toCensusResponse(census)
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// toCensusResponse renders the replicated census: per topic, the shapes in
// first-seen order, the derived path view, and the draft schema (rendered at
// read time so drafter improvements apply to already-taken censuses).
func toCensusResponse(c *cluster.IngestableCensus) map[string]*TopicCensusResponse {
	out := make(map[string]*TopicCensusResponse, len(c.Topics))
	for topic, tc := range c.Topics {
		r := &TopicCensusResponse{
			RefreshEpoch: c.RefreshEpoch,
			Rows:         tc.Rows,
			Shapes:       make([]ShapeCensusResponse, 0, len(tc.Shapes)),
			Paths:        tc.PathsView(),
		}
		for fp, s := range tc.Shapes {
			r.Shapes = append(r.Shapes, ShapeCensusResponse{
				Fingerprint: fp, Shape: s.Shape, Count: s.Count,
				FirstRow: s.FirstRow, LastRow: s.LastRow,
			})
		}
		sort.Slice(r.Shapes, func(i, j int) bool { return r.Shapes[i].FirstRow < r.Shapes[j].FirstRow })
		if draft, err := tc.DraftTypeSchema(); err == nil {
			r.DraftSchema = string(draft)
		}
		out[topic] = r
	}
	return out
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
		WorkerState:        st.WorkerState,
		Phase:              st.Phase,
		SnapshotProgress:   make([]TableSnapshotProgress, 0, len(st.SnapshotProgress)),
		Position:           st.Position,
		Lag:                st.Lag,
		LagUnit:            st.LagUnit,
		CaughtUp:           st.CaughtUp,
		ReSnapshotRequired: st.ReSnapshotRequired,
	}
	for _, t := range st.SnapshotProgress {
		resp.SnapshotProgress = append(resp.SnapshotProgress, TableSnapshotProgress{
			Table:       t.Table,
			Topic:       t.Topic,
			Complete:    t.Complete,
			ChunksTotal: t.ChunksTotal,
			ChunksDone:  t.ChunksDone,
		})
	}
	return resp
}
