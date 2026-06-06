package http

import (
	"encoding/json"
	"errors"
	httpgo "net/http"
	"strconv"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// defaultSyncableErrorsLimit is the page size GetSyncableErrors uses when
// the caller omits ?limit. Storage clamps the upper bound, so a caller
// asking for more just gets a full page.
const defaultSyncableErrorsLimit = 100

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

	limit := defaultSyncableErrorsLimit
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
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve syncable errors")
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
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to dead-letter the stuck proposal")
		return
	}

	// 202: the worker honors the request on its next retry, so the
	// dead-letter record lands shortly after this returns. The body names
	// the targeted index; poll GET /syncable/{id}/errors to confirm.
	bs, err := json.Marshal(SyncableDeadLetterStuckResponse{Index: index})
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to marshal response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpgo.StatusAccepted)
	_, _ = w.Write(bs)
}

// SyncableStatusResponse is the lean operational status of a syncable's
// worker — currently just whether it is blocked, and if so on what.
type SyncableStatusResponse struct {
	Stuck bool `json:"stuck"`
	// Index, Since, and Message are present only when stuck.
	Index   uint64 `json:"index,omitempty"`
	Since   string `json:"since,omitempty"`
	Message string `json:"message,omitempty"`
}

// GetSyncableStatus reports whether a syncable's worker is currently blocked
// retrying a transient error (GET /syncable/{id}/status). Backed by
// replicated state, so any node answers identically — this is how an operator
// (or a dashboard) discovers a wedged syncable behind a load balancer, and
// the index to expect when they POST .../deadletter/.
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
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve syncable status")
		return
	}

	resp := SyncableStatusResponse{Stuck: ok}
	if ok {
		resp.Index = stuck.Index
		resp.Since = time.Unix(0, stuck.SinceUnixNano).UTC().Format(time.RFC3339Nano)
		resp.Message = stuck.Message
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to marshal response")
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
			"the syncable rejected the proposal again; dead letter left in place", capError(err))
	default:
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to replay the dead-lettered proposal")
	}
}

// capError bounds an error string before it goes into a response detail so a
// chatty driver error can't bloat the body.
func capError(err error) string {
	const max = 512
	s := err.Error()
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}
