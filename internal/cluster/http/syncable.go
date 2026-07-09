package http

import (
	"encoding/json"
	"errors"
	httpgo "net/http"
	"strconv"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// defaultErrorsPageLimit is the page size the dead-letter listing handlers
// (GetSyncableErrors, GetTypeMigrationErrors) use when the caller omits
// ?limit. Storage clamps the upper bound, so a caller asking for more just
// gets a full page.
const defaultErrorsPageLimit = 100

// AddSyncable (POST /syncable/{id}) and GetSyncables (GET /syncable) are
// served by the generic config handlers in config_handlers.go. The handlers
// below are syncable-specific: dead-letter inspection, manual dead-lettering,
// status, and replay.

// SyncableDeadLetterResponse is one dead-lettered proposal — a proposal a
// syncable permanently skipped. index is the raft index of the dropped
// proposal (the proposal itself stays in the log); pass it back as
// ?since to page forward.
type SyncableDeadLetterResponse struct {
	Index     uint64 `json:"index"`
	Timestamp string `json:"timestamp"`
	Kind      string `json:"kind"`
	Message   string `json:"message"`
}

// GetSyncableErrors returns the dead-letter records for a syncable —
// proposals it permanently skipped — in ascending raft-index order.
// Query params: ?since=<raft index> (exclusive cursor, default 0) and
// ?limit=<n> (page size, default 100; storage caps the maximum). The
// records are replicated state, so any node answers identically.
func (h *HTTP) GetSyncableErrors(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	var since uint64
	if qs := r.URL.Query().Get("since"); qs != "" {
		n, err := strconv.ParseUint(qs, 10, 64)
		if err != nil {
			writeErrorf(w, httpgo.StatusBadRequest, "invalid_parameter", "since parameter %q is not a valid raft index", qs)
			return
		}
		since = n
	}

	limit := defaultErrorsPageLimit
	if ql := r.URL.Query().Get("limit"); ql != "" {
		n, err := strconv.Atoi(ql)
		if err != nil || n <= 0 {
			writeErrorf(w, httpgo.StatusBadRequest, "invalid_parameter", "limit parameter %q is not a positive integer", ql)
			return
		}
		limit = n
	}

	if !h.linearize(w, r) {
		return
	}

	dls, err := h.c.SyncableDeadLetters(id, since, limit)
	if err != nil {
		writeInternalError(w, "failed to retrieve syncable errors", err)
		return
	}

	body := make([]SyncableDeadLetterResponse, 0, len(dls))
	for _, d := range dls {
		body = append(body, SyncableDeadLetterResponse{
			Index:     d.Index,
			Timestamp: time.Unix(0, d.TimestampUnixNano).UTC().Format(time.RFC3339Nano),
			Kind:      d.Kind,
			Message:   d.Message,
		})
	}

	writeArrayBody(w, body)
}

// SyncableDeadLetterStuckResponse reports which proposal a manual dead-letter
// request targeted — the raft index the syncable was blocked on.
type SyncableDeadLetterStuckResponse struct {
	Index uint64 `json:"index"`
}

// DeadLetterStuckSyncable skips the proposal a syncable is currently blocked
// retrying (POST /syncable/{id}/deadletter/). A transient sync error retries
// forever by design, so a syncable wedged on a proposal the downstream will
// never accept stalls visibly rather than losing data; this is the operator's
// lever to dead-letter that one proposal (kind "manual") and let the worker
// advance.
//
// Node-agnostic: the stuck state is replicated, so any node can find the
// blocked index and propose the skip through Raft. A syncable that isn't
// currently blocked gets 409. The worker honors the request on its next
// retry; poll GET /syncable/{id}/errors to confirm the manual dead letter.
func (h *HTTP) DeadLetterStuckSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	index, err := h.c.DeadLetterStuckSyncable(r.Context(), id)
	if err != nil {
		if errors.Is(err, cluster.ErrSyncNotStuck) {
			writeError(w, httpgo.StatusConflict, "not_stuck",
				"syncable is not currently blocked; it may be healthy or unknown")
			return
		}
		writeInternalError(w, "failed to dead-letter the stuck proposal", err)
		return
	}

	// 202: the worker honors the request on its next retry, so the
	// dead-letter record lands shortly after this returns. The body names
	// the targeted index; poll GET /syncable/{id}/errors to confirm.
	bs, err := json.Marshal(SyncableDeadLetterStuckResponse{Index: index})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpgo.StatusAccepted)
	_, _ = w.Write(bs)
}

// SyncableDeleteResponse confirms a syncable was deleted and whether the
// operator asked to keep its destination state.
type SyncableDeleteResponse struct {
	ID       string `json:"id"`
	KeepData bool   `json:"keepData"`
}

// DeleteSyncable (DELETE /syncable/{id}) removes a syncable: its config and
// checkpoint are deleted atomically (consensus), the worker is stopped, and —
// unless ?keepData=true — the owner tears down the syncable's destination
// best-effort (for a SQL syncable, dropping its table). A later same-name POST
// then starts fresh from index 0.
//
// The logical deletion is the authoritative act and completes before this
// returns (Propose blocks until the Actual applies); the destination teardown
// is a best-effort side effect that settles shortly after on the leader. The
// route is leader-pinned so this runs where the teardown does.
func (h *HTTP) DeleteSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	keepData := false
	if raw := r.URL.Query().Get("keepData"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "keepData must be a boolean")
			return
		}
		keepData = v
	}

	if err := h.c.DeleteSyncable(r.Context(), id, keepData); err != nil {
		writeProposeError(w, err, "syncable", "delete syncable")
		return
	}

	bs, err := json.Marshal(SyncableDeleteResponse{ID: id, KeepData: keepData})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpgo.StatusOK)
	_, _ = w.Write(bs)
}

// RebuildSyncable (POST /syncable/{id}/rebuild) re-materializes a syncable's
// destination in place from index 0, keeping the same config. The destination
// (for a SQL syncable, its table) is torn down, recreated empty, and refilled
// by replaying the log — the recovery primitive for a drifted/corrupted
// projection. Schema changes are NOT done this way (use DELETE then POST).
// Returns 404 if the syncable is unknown. Leader-pinned.
func (h *HTTP) RebuildSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	if err := h.c.RebuildSyncable(r.Context(), id); err != nil {
		if errors.Is(err, cluster.ErrResourceNotFound) {
			writeError(w, httpgo.StatusNotFound, "not_found", "syncable not found")
			return
		}
		writeProposeError(w, err, "syncable", "rebuild syncable")
		return
	}

	// 202: the checkpoint reset and destination teardown/re-init are done, but
	// the replay that refills the destination runs in the worker afterward.
	// Poll GET /syncable/{id}/status (lag → 0) to see the rebuild complete.
	w.WriteHeader(httpgo.StatusAccepted)
}

// SyncableStatusResponse is the operational status of a syncable's worker:
// whether it is blocked (and on what), plus its numeric progress (how far it
// has synced vs. how far there is to sync).
type SyncableStatusResponse struct {
	Stuck bool `json:"stuck"`
	// Index, Since, and Message are present only when stuck.
	Index   uint64 `json:"index,omitempty"`
	Since   string `json:"since,omitempty"`
	Message string `json:"message,omitempty"`

	// Progress fields are ALWAYS present (not only when stuck). Distinct
	// from Index above, which is the stuck-only "index it's blocked on".
	//
	// CheckpointIndex is the persisted SyncableIndex — the consumed head the
	// worker has synced, topic-skipped, or dead-lettered through.
	// HeadIndex is DataEventIndex — the highest data (non-metadata) raft
	// index on this node, the head the worker converges to at EOF. Lag is
	// max(0, HeadIndex − CheckpointIndex) and CaughtUp is Lag == 0, which is
	// true exactly when the worker has nothing left to process. See the
	// consumed-head semantics in syncable-progress-lag.
	CheckpointIndex uint64 `json:"checkpoint_index"`
	HeadIndex       uint64 `json:"head_index"`
	Lag             uint64 `json:"lag"`
	CaughtUp        bool   `json:"caught_up"`
}

// GetSyncableStatus reports a syncable worker's operational status
// (GET /syncable/{id}/status): whether it is blocked retrying a transient
// error, and its numeric progress — checkpoint_index / head_index / lag /
// caught_up. Backed by replicated + local apply state read behind the same
// linearize barrier, so any node answers identically and without a leader
// hop — this is how an operator (or a dashboard) discovers a wedged syncable
// behind a load balancer (and the index to expect when they POST
// .../deadletter/), and answers "is it caught up?" (lag == 0).
func (h *HTTP) GetSyncableStatus(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}

	if !h.linearize(w, r) {
		return
	}

	stuck, ok, err := h.c.SyncableStuck(id)
	if err != nil {
		writeInternalError(w, "failed to retrieve syncable status", err)
		return
	}

	checkpoint, head, err := h.c.SyncableProgress(id)
	if err != nil {
		writeInternalError(w, "failed to retrieve syncable progress", err)
		return
	}

	resp := SyncableStatusResponse{Stuck: ok}
	if ok {
		resp.Index = stuck.Index
		resp.Since = time.Unix(0, stuck.SinceUnixNano).UTC().Format(time.RFC3339Nano)
		resp.Message = stuck.Message
	}

	// Both numbers are sourced behind the same linearize barrier above, so
	// checkpoint (replicated apply state) and head (local apply state) are
	// mutually consistent and head ≥ checkpoint on the default path. Clamp
	// anyway as belt-and-suspenders, and for ?consistency=stale reads on a
	// lagging follower where the two could momentarily cross.
	resp.CheckpointIndex = checkpoint
	resp.HeadIndex = head
	if head > checkpoint {
		resp.Lag = head - checkpoint
	}
	resp.CaughtUp = resp.Lag == 0

	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// ReplaySyncableDeadLetter re-drives a dead-lettered proposal
// (POST /syncable/{id}/replay/{index}): it re-runs the syncable's Sync for the
// proposal at `index` and, on success, clears the dead-letter record. Use it
// after fixing the downstream that caused the original skip. Node-agnostic.
func (h *HTTP) ReplaySyncableDeadLetter(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}
	index, err := strconv.ParseUint(r.PathValue("index"), 10, 64)
	if err != nil {
		writeErrorf(w, httpgo.StatusBadRequest, "invalid_parameter",
			"index %q is not a valid raft index", r.PathValue("index"))
		return
	}

	err = h.c.ReplaySyncableDeadLetter(r.Context(), id, index)
	switch {
	case err == nil:
		w.WriteHeader(httpgo.StatusOK)
	case errors.Is(err, cluster.ErrNotDeadLettered):
		writeError(w, httpgo.StatusNotFound, "not_dead_lettered",
			"raft index is not a dead letter for this syncable")
	case errors.Is(err, cluster.ErrReplaySyncFailed):
		// The downstream rejected the proposal again; the dead letter is left
		// in place. Surface the cause so the operator can see what failed.
		writeErrorWithDetails(w, httpgo.StatusBadGateway, "replay_failed",
			"the syncable rejected the proposal again; dead letter left in place", redactedDetail(err))
	default:
		writeInternalError(w, "failed to replay the dead-lettered proposal", err)
	}
}

// capString bounds a detail string before it goes into a response body so a
// chatty error can't bloat it.
func capString(s string) string {
	const max = 512
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

func capError(err error) string { return capString(err.Error()) }

// redactedDetail returns an error string safe to put in a response body: if the
// error chain carries a cluster.RedactedError (a driver or migration error that
// may echo entity PII in its full text), only its PII-free RedactedMessage is
// exposed; otherwise the committed-authored error text is used. The full detail
// is kept in this node's logs (see the db replay path). Use this — not capError
// — for any detail derived from a Sync/apply error.
func redactedDetail(err error) string {
	if red, ok := errors.AsType[cluster.RedactedError](err); ok {
		return capString(red.RedactedMessage())
	}
	return capError(err)
}
