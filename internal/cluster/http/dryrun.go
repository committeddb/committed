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
const dryRunMaxEntriesCap = 1_000_000

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
	ctx, cancel := context.WithTimeout(r.Context(), dryRunTimeout)
	defer cancel()
	rep, err := h.c.DryRunSyncable(ctx, mimeType, body, opts)
	if err != nil {
		h.writeReadError(w, r, err, "invalid_config", "dry-run syncable")
		return
	}
	writeJSONStatus(w, httpgo.StatusOK, rep)
}
