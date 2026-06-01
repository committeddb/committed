package http

import (
	httpgo "net/http"
	"strconv"
	"time"
)

// defaultSyncableErrorsLimit is the page size GetSyncableErrors uses when
// the caller omits ?limit. Storage clamps the upper bound, so a caller
// asking for more just gets a full page.
const defaultSyncableErrorsLimit = 100

func (h *HTTP) AddSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid syncable configuration")
		return
	}

	if err := h.c.ProposeSyncable(r.Context(), c); err != nil {
		writeProposeError(w, err, "syncable", "propose syncable")
		return
	}

	// See database.go AddDatabase for the G705 rationale.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(c.ID)) //nolint:gosec // G705
}

func (h *HTTP) GetSyncables(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Syncables()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve syncables")
		return
	}

	writeConfigurations(w, cfgs)
}

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
