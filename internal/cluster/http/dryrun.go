package http

import (
	"context"
	"fmt"
	"io"
	"mime"
	httpgo "net/http"
	"strconv"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// dryRunTimeout caps a dry-run's fold work server-side; the sampling
// budget bounds it far earlier in practice.
const dryRunTimeout = 2 * time.Minute

// dryRunMaxEntriesCap keeps a caller from turning the diagnostic into a
// full replay.
const dryRunMaxEntriesCap = 10_000_000

// dryRunTimeoutCap bounds caller-requested timeouts.
const dryRunTimeoutCap = 10 * time.Minute

// DryRunSyncable (POST /syncable/dryrun) rehearses a syncable config
// against a bounded sample of the committed log and returns the
// diagnostic report — counters, per-join resolution, sample outputs,
// and auto-generated silent-empty findings. Nothing is admitted or
// stored, no destination is touched: the instrument exists so "valid
// config, wrong result, no error" costs minutes, not a full replay.
// Query: maxEntries (default 100000), fromIndex (default: evenly-spaced
// multi-window sampling across the whole log).
func (h *HTTP) DryRunSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	mimeType := "text/toml"
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err == nil && mt != "" {
		mimeType = mt
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeReadError(w, r, err, "invalid_config", "read dry-run body")
		return
	}
	opts := cluster.DryRunOptions{}
	if q := r.URL.Query().Get("maxEntries"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n <= 0 || n > dryRunMaxEntriesCap {
			h.writeReadError(w, r, fmt.Errorf("maxEntries must be 1..%d", dryRunMaxEntriesCap), "invalid_config", "invalid maxEntries")
			return
		}
		opts.MaxEntries = n
	}
	if q := r.URL.Query().Get("fromIndex"); q != "" {
		n, err := strconv.ParseUint(q, 10, 64)
		if err != nil {
			h.writeReadError(w, r, err, "invalid_config", "invalid fromIndex")
			return
		}
		opts.FromIndex = n
	}
	timeout := dryRunTimeout
	if q := r.URL.Query().Get("timeoutSeconds"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n <= 0 || time.Duration(n)*time.Second > dryRunTimeoutCap {
			h.writeReadError(w, r, fmt.Errorf("timeoutSeconds must be 1..%d", int(dryRunTimeoutCap/time.Second)), "invalid_config", "invalid timeoutSeconds")
			return
		}
		timeout = time.Duration(n) * time.Second
	}
	opts.Timeout = timeout
	// The server's global WriteTimeout (default 120s) starts counting at
	// the request read — equal to the default compute budget, so a
	// rehearsal using its whole budget built its (partial) report only
	// to find the connection already dead: the field's "empty reply
	// after exactly 120s". Extend THIS response's write deadline past
	// the compute deadline; best-effort (an unsupported wrapper keeps
	// the old behavior).
	_ = httpgo.NewResponseController(w).SetWriteDeadline(time.Now().Add(timeout + 30*time.Second))
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	rep, err := h.c.DryRunSyncable(ctx, mimeType, body, opts)
	if err != nil {
		// The dry-run IS the authoring loop: a rejection must carry the
		// parser's actual words, not a generic label (field-reported —
		// authors had to POST to the real endpoint to learn the error).
		writeErrorf(w, httpgo.StatusBadRequest, "invalid_config", "dry-run: %s", err)
		return
	}
	writeJSONStatus(w, httpgo.StatusOK, rep)
}
