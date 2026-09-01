package http

import (
	"errors"
	"io"
	httpgo "net/http"
	"net/url"
	"path"
	"time"
)

// defaultProxyTimeout bounds a member→member proxy hop (follower→leader for a
// leader-only read, or any-node→owner for an owner-local read). Sized like
// defaultReadIndexTimeout — well under the server WriteTimeout — so a wedged
// or unreachable target yields a clean local answer (or 503) rather than
// hanging the caller's connection.
const defaultProxyTimeout = 5 * time.Second

// forwardedHeader marks a request one node has already proxied to another. If
// such a request lands on a node that still isn't the one that should answer
// (the forwarder's leader/owner view was stale, or leadership is flapping),
// the receiver answers as best it can locally instead of forwarding again — a
// one-hop guard against proxy loops. The caller retries; by then the view has
// usually settled.
const forwardedHeader = "X-Committed-Forwarded"

// leaderRead wraps a handler that only the raft leader can answer correctly
// because it reads leader-only state (e.g. per-member replication progress,
// which etcd raft tracks only on the leader). On the leader it serves locally;
// on a follower it reverse-proxies the request to the leader's advertised API
// URL, so a caller behind a load balancer gets a leader-truthful answer no
// matter which node it reaches.
//
// It returns 503 (with the believed leader id in the error details) when:
// there is no known leader; the leader has not announced an API URL (no
// COMMITTED_API_URL — the documented degraded path); the request already
// carries the loop-guard marker; or the leader can't be reached before the
// deadline. The caller can retry, or use leaderId to target the leader
// directly. See raft-leader-read-proxy.md.
// syncableOwnerRoute routes an owner-local syncable verb (rebuild,
// rematerialize) to the node whose worker serves the syncable. For an
// unpinned syncable that is the leader — leaderRead exactly, byte-identical
// to pre-zone behavior. For a zone-pinned syncable it is the pinned owner:
// the verbs' worker drain must run WHERE THE WORKER RUNS (the
// drain-before-reset ordering that keeps a stale checkpoint bump from
// defeating the replay only holds when the drain and the reset proposal
// originate on the worker's node — per-peer FIFO transport then preserves
// their order through proposal forwarding). One bounded hop with the
// standard loop guard; an unsatisfiable pin is refused up front (there is
// no worker anywhere to drain or replay).
func (h *HTTP) syncableOwnerRoute(next httpgo.HandlerFunc) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		id := r.PathValue("id")
		zone, unsatisfiable, pinned := h.db.SyncableZonePin(id)
		if !pinned {
			h.leaderRead(next)(w, r)
			return
		}
		if unsatisfiable {
			writeError(w, httpgo.StatusServiceUnavailable, "pin_unsatisfiable",
				"the pin to zone \""+zone+"\" has no serving node; restore a node in the zone (or re-POST the config without `zone`) and retry")
			return
		}
		owner := h.db.SyncableOwner(id)
		if owner == h.db.ID() {
			next(w, r)
			return
		}
		if r.Header.Get(forwardedHeader) != "" {
			// Already forwarded once and we're still not the owner: a stale
			// routing view mid-ownership-change. Retryable, never a loop.
			writeError(w, httpgo.StatusServiceUnavailable, "not_syncable_owner",
				"ownership moved while routing; retry")
			return
		}
		if ownerURL, ok := h.db.MemberAPIURL(owner); ok && ownerURL != "" {
			if err := h.proxyToMember(w, r, ownerURL); err == nil {
				return
			}
			writeError(w, httpgo.StatusServiceUnavailable, "owner_unreachable",
				"the pinned owner's API did not answer; check the node and retry")
			return
		}
		writeError(w, httpgo.StatusServiceUnavailable, "owner_unreachable",
			"the pinned owner has not announced an API URL; set COMMITTED_API_URL on every node")
	}
}

func (h *HTTP) leaderRead(next httpgo.HandlerFunc) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		self := h.c.ID()
		leaderID := h.c.Leader()

		if leaderID != 0 && leaderID == self {
			next(w, r)
			return
		}

		// Already proxied once and still not the leader → don't forward
		// again; let the caller retry against a settled leader view.
		if r.Header.Get(forwardedHeader) != "" {
			writeLeaderUnavailable(w, leaderID, "request was forwarded but this node is not the leader")
			return
		}
		if leaderID == 0 {
			writeLeaderUnavailable(w, leaderID, "no raft leader is currently known")
			return
		}

		leaderURL, ok := h.c.MemberAPIURL(leaderID)
		if !ok || leaderURL == "" {
			writeLeaderUnavailable(w, leaderID,
				"the leader has not announced an API address (set COMMITTED_API_URL); target the leader directly")
			return
		}

		if err := h.proxyToMember(w, r, leaderURL); err != nil {
			// proxyToMember failed before writing anything, so the 503 is
			// still ours to write. Leader-only state has no local fallback —
			// this wrapper's hard-fail contract (vs. the syncable status
			// handler's soft degrade, which serves replicated fields instead).
			writeLeaderUnavailable(w, leaderID, "could not reach the leader: "+err.Error())
		}
	}
}

// proxyToMember forwards r to the member API at baseURL and copies the
// response back verbatim (status, Content-Type, body). A returned error means
// NOTHING was written to w — the target URL didn't parse, the request
// couldn't be built, or the member couldn't be reached — so the caller picks
// the fallback its endpoint's contract requires: leaderRead hard-fails with
// 503 (leader-only state has no substitute), the syncable status handler
// soft-degrades to its local answer (replicated fields minus the owner-local
// one). Once the member responds, the copy is committed and any error is the
// caller's connection's problem.
func (h *HTTP) proxyToMember(w httpgo.ResponseWriter, r *httpgo.Request, baseURL string) error {
	// The scheme and host are fixed by the member's announced URL — trusted,
	// replicated state, not request input. Only the path and query come from
	// the request, joined onto the trusted base, so a crafted request can't
	// redirect the hop to an arbitrary host (no SSRF).
	base, err := url.Parse(baseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return errors.New("the member's announced API address is not a valid URL")
	}
	base.Path = path.Join(base.Path, r.URL.Path)
	base.RawQuery = r.URL.RawQuery

	//nolint:gosec // G704: target host/scheme are the member's replicated API URL, not request input; only path+query are forwarded.
	req, err := httpgo.NewRequestWithContext(r.Context(), r.Method, base.String(), r.Body)
	if err != nil {
		return errors.New("could not build the proxy request")
	}
	// Forward the headers that matter for auth, content negotiation, and
	// tracing; set the loop-guard marker so the receiving member (or a
	// stale-view wrong member) can tell this is a forwarded request.
	copyHeader(req.Header, r.Header, "Authorization", "Content-Type", "Accept", "X-Request-ID")
	req.Header.Set(forwardedHeader, "1")

	resp, err := h.proxyClient.Do(req) //nolint:gosec // G704: target is the trusted member URL (see above).
	if err != nil {
		return errors.New("no response from the member's API")
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return nil
}

// copyHeader copies the named headers from src to dst when present.
func copyHeader(dst, src httpgo.Header, keys ...string) {
	for _, k := range keys {
		if v := src.Get(k); v != "" {
			dst.Set(k, v)
		}
	}
}

// writeLeaderUnavailable writes a 503 carrying the believed leader id, so a
// caller behind a load balancer can target the leader directly (or retry).
func writeLeaderUnavailable(w httpgo.ResponseWriter, leaderID uint64, message string) {
	writeErrorWithDetails(w, httpgo.StatusServiceUnavailable, "leader_unavailable",
		message, map[string]any{"leaderId": leaderID})
}
