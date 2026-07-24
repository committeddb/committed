package http

import (
	"encoding/json"
	"errors"
	httpgo "net/http"
	"slices"

	"github.com/committeddb/committed/internal/cluster"
)

// PipelineStatusResponse is the end-to-end view of one topic's data flow — its
// producing ingestable (if any), the data head, and every syncable consuming the
// topic — composed so an operator can answer "why isn't my data showing up
// downstream?" in one call instead of stitching GET /ingestable/{id}/status and
// GET /syncable/{id}/status together by topic. It is anchored on the topic (the
// type id), so it also covers a topic fed by direct proposals rather than an
// ingestable.
type PipelineStatusResponse struct {
	// Topic is the type id this pipeline flows through — the path key.
	Topic string `json:"topic"`
	// HeadIndex is the data head (DataEventIndex) — the convergence target the
	// producer and the consumers are racing toward.
	HeadIndex uint64 `json:"headIndex"`
	// Ingestable is the id of the ingestable producing this topic, or "" when the
	// topic is fed by direct proposals (no ingestable).
	Ingestable string `json:"ingestable,omitempty"`
	// Ingest is the producer's ingest status, present only when an ingestable
	// produces this topic and its status was retrievable.
	Ingest *IngestableStatusResponse `json:"ingest,omitempty"`
	// IngestError is set when a producing ingestable exists but its status could
	// not be read — e.g. no ingestable worker is running on the node that
	// answered. The producer is still named (fail loud) and the pipeline's
	// overall caughtUp is forced false.
	IngestError string `json:"ingestError,omitempty"`
	// Syncables is every syncable consuming the topic, each with its own lag.
	Syncables []PipelineSyncableStatus `json:"syncables"`
	// CaughtUp is true only when the producer (if any) is caught up AND every
	// consuming syncable is caught up — the whole pipeline is at rest.
	CaughtUp bool `json:"caughtUp"`
}

// PipelineSyncableStatus is one consumer's place in the pipeline: how far it has
// checkpointed, how far behind the head it is, and whether it is caught up.
type PipelineSyncableStatus struct {
	ID string `json:"id"`
	// WorkerState ("running" or "parked") and Stuck mirror GET /syncable/{id}/status
	// so the pipeline view is not less informative than the single-resource one — a
	// parked or transiently-stuck consumer is visible here, not just as bare lag.
	WorkerState     string `json:"workerState"`
	Stuck           bool   `json:"stuck"`
	CheckpointIndex uint64 `json:"checkpointIndex"`
	Lag             uint64 `json:"lag"`
	CaughtUp        bool   `json:"caughtUp"`
	// Error is set when this consumer's progress couldn't be read. The consumer
	// is still listed (fail loud, not silently dropped) and the pipeline's
	// overall caughtUp is forced false — an unknown consumer can't be at rest.
	Error string `json:"error,omitempty"`
}

// GetPipelineStatus (GET /type/{id}/pipeline) composes the end-to-end status of
// the pipeline flowing through topic {id} (the type id): its producing
// ingestable's status (if any), the data head, and every syncable consuming the
// topic, with an overall caught-up flag. The linkage (producing ingestable →
// topic → consuming syncables) is resolved server-side from the stored configs;
// the caller passes only the topic. It reuses the same status computations as the
// single-resource endpoints, so the numbers match.
func (h *HTTP) GetPipelineStatus(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "id is empty")
		return
	}
	if !h.linearize(w, r) {
		return
	}
	topic := id

	// 1. The topic must be a registered type — this is /type/{id}/pipeline.
	types, err := h.c.Types()
	if err != nil {
		writeInternalError(w, "failed to list types", err)
		return
	}
	typeExists := false
	for _, c := range types {
		if c.ID == id {
			typeExists = true
			break
		}
	}
	if !typeExists {
		writeError(w, httpgo.StatusNotFound, "type_not_found",
			"no type is configured for this id")
		return
	}

	// 2. The producing ingestable (optional) and its ingest status. A topic fed
	// by direct proposals has no ingestable.
	ings, err := h.c.Ingestables()
	if err != nil {
		writeInternalError(w, "failed to list ingestables", err)
		return
	}
	var producerID string
	for _, c := range ings {
		if slices.Contains(topicsOf(c, "ingestable"), topic) {
			producerID = c.ID
			break
		}
	}

	var ingest *IngestableStatusResponse
	var ingestErr string
	caughtUp := true
	if producerID != "" {
		st, err := h.c.IngestableStatus(r.Context(), producerID)
		switch {
		case errors.Is(err, cluster.ErrIngestableNotRunning):
			// Fail loud: a producer exists but its status is unknown on the node
			// that answered, so the pipeline can't be considered at rest.
			ingestErr = "no ingestable worker is running for this id on the node that answered"
			caughtUp = false
		case err != nil:
			writeInternalError(w, "failed to retrieve ingestable status", err)
			return
		default:
			resp := toIngestableStatusResponse(st)
			ingest = &resp
			caughtUp = st.CaughtUp
		}
	}

	// 3. Every syncable consuming the topic, measured against the data head.
	syncs, err := h.c.Syncables()
	if err != nil {
		writeInternalError(w, "failed to list syncables", err)
		return
	}

	// The data head (DataEventIndex) is id-independent, so read it once up front
	// and measure every consumer against the same snapshot — this also surfaces
	// the head even when no consumer reports (none configured, or all error).
	_, head, _ := h.c.SyncableProgress(id)

	consumers := make([]PipelineSyncableStatus, 0)
	for _, c := range syncs {
		if !slices.Contains(topicsOf(c, "syncable"), topic) {
			continue
		}
		// Worker state (replicated) is read separately from progress, so a consumer
		// whose progress read fails still reports whether its worker parked.
		st, stOK, _ := h.c.SyncableStuck(c.ID)
		workerState := cluster.WorkerStateRunning
		if stOK && st.Parked {
			workerState = cluster.WorkerStateParked
		}
		stuck := stOK && !st.Parked

		checkpoint, _, err := h.c.SyncableProgress(c.ID)
		if err != nil {
			// Fail loud: list the consumer with its error rather than dropping
			// it, and force the pipeline's caughtUp false — we can't claim the
			// pipeline is at rest with a consumer whose progress is unknown.
			// redactedDetail, not err.Error(): a progress-read error may wrap a
			// driver error that echoes connection identity or a bound value.
			consumers = append(consumers, PipelineSyncableStatus{ID: c.ID, WorkerState: workerState, Stuck: stuck, Error: redactedDetail(err)})
			caughtUp = false
			continue
		}
		lag := uint64(0)
		if head > checkpoint {
			lag = head - checkpoint
		}
		cu := lag == 0
		if !cu {
			caughtUp = false
		}
		consumers = append(consumers, PipelineSyncableStatus{
			ID:              c.ID,
			WorkerState:     workerState,
			Stuck:           stuck,
			CheckpointIndex: checkpoint,
			Lag:             lag,
			CaughtUp:        cu,
		})
	}

	resp := PipelineStatusResponse{
		Topic:       topic,
		HeadIndex:   head,
		Ingestable:  producerID,
		Ingest:      ingest,
		IngestError: ingestErr,
		Syncables:   consumers,
		CaughtUp:    caughtUp,
	}
	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// topicsOf extracts the topic(s) a stored config references. kind is the config
// kind ("ingestable" or "syncable"); the config's type lives at {kind}.type and
// its topic at {type}.topic (sql → sql.topic, http → http.topic, sql-projection
// → sql-projection.topic). A multi-source sql-projection lists its topics in
// [[sql-projection.source]] blocks, so those are included too. Returns nil if
// the config can't be parsed — a degraded config drops out of the linkage rather
// than erroring the whole view.
func topicsOf(c *cluster.Configuration, kind string) []string {
	v, err := cluster.ParseConfigBytes(c.MimeType, c.Data)
	if err != nil {
		return nil
	}
	typ := v.GetString(kind + ".type")

	seen := map[string]bool{}
	var topics []string
	add := func(t string) {
		if t != "" && !seen[t] {
			seen[t] = true
			topics = append(topics, t)
		}
	}

	add(v.GetString(typ + ".topic"))
	if typ == "sql-projection" {
		var srcs []struct {
			Topic string `mapstructure:"topic"`
		}
		_ = v.UnmarshalKey("sql-projection.source", &srcs)
		for _, s := range srcs {
			add(s.Topic)
		}
	}
	return topics
}
