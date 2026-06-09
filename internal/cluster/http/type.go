package http

import (
	"errors"
	httpgo "net/http"
	"strconv"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// AddType (POST /type/{id}) and GetTypes (GET /type) are served by the
// generic config handlers in config_handlers.go. The handlers below are
// type-specific: runtime migration-failure inspection and retry, the
// type-keyed twins of the syncable dead-letter endpoints in syncable.go.

// TypeMigrationErrorResponse is one runtime migration failure — a proposal
// whose entity the type's migration program (jq) could not transform.
// index is the raft index of the proposal (the data itself stays in the
// log); pass it back as ?since to page forward. fromVersion -> toVersion
// identify the failing chain step: the toVersion program is the one that
// errored.
type TypeMigrationErrorResponse struct {
	Index       uint64 `json:"index"`
	FromVersion int    `json:"fromVersion"`
	ToVersion   int    `json:"toVersion"`
	Timestamp   string `json:"timestamp"`
	Message     string `json:"message"`
}

// GetTypeMigrationErrors returns the runtime migration failures recorded
// for a type — proposals whose entities its migration program broke on —
// in ascending raft-index order (GET /type/{id}/migration-errors). Query
// params: ?since=<raft index> (exclusive cursor, default 0) and ?limit=<n>
// (page size, default 100; storage caps the maximum). The records are
// replicated state, so any node answers identically.
func (h *HTTP) GetTypeMigrationErrors(w httpgo.ResponseWriter, r *httpgo.Request) {
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

	dls, err := h.c.TypeMigrationDeadLetters(id, since, limit)
	if err != nil {
		writeInternalError(w, "failed to retrieve type migration errors", err)
		return
	}

	body := make([]TypeMigrationErrorResponse, 0, len(dls))
	for _, d := range dls {
		body = append(body, TypeMigrationErrorResponse{
			Index:       d.Index,
			FromVersion: d.FromVersion,
			ToVersion:   d.ToVersion,
			Timestamp:   time.Unix(0, d.TimestampUnixNano).UTC().Format(time.RFC3339Nano),
			Message:     d.Message,
		})
	}

	writeArrayBody(w, body)
}

// ReplayTypeMigrationDeadLetter re-runs the type's (presumably fixed)
// migration chain for the dead-lettered proposal at `index` and, on
// success, clears the record (POST /type/{id}/migration-retry/{index}).
// Use it after fixing the program (re-POST the type with a corrected
// transform) to verify the fix against the exact payload that broke the old
// one. It does not deliver the result downstream — the proposal is still
// dead-lettered for every syncable that skipped it; replay those with
// POST /syncable/{id}/replay/{index}, which re-runs the same chain on the
// way through. Node-agnostic.
func (h *HTTP) ReplayTypeMigrationDeadLetter(w httpgo.ResponseWriter, r *httpgo.Request) {
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

	err = h.c.ReplayTypeMigrationDeadLetter(r.Context(), id, index)
	switch {
	case err == nil:
		w.WriteHeader(httpgo.StatusOK)
	case errors.Is(err, cluster.ErrNotDeadLettered):
		writeError(w, httpgo.StatusNotFound, "not_dead_lettered",
			"raft index is not a migration dead letter for this type")
	case errors.Is(err, cluster.ErrReplayMigrationFailed):
		// The chain still fails on this payload; the record is left in
		// place. Surface the cause so the operator can see what failed.
		writeErrorWithDetails(w, httpgo.StatusBadGateway, "migration_retry_failed",
			"the migration chain still fails on this proposal; dead letter left in place", capError(err))
	default:
		writeInternalError(w, "failed to retry the migration", err)
	}
}
