package http

import (
	"io"
	httpgo "net/http"
	"net/url"
	"path"
	"time"
)

// defaultProxyTimeout bounds a follower→leader proxy hop for a leader-only
// read. Sized like defaultReadIndexTimeout — well under the server
// WriteTimeout — so a wedged or unreachable leader yields a clean 503 rather
// than hanging the caller's connection.
const defaultProxyTimeout = 5 * time.Second

// forwardedHeader marks a request a follower has already proxied to the
// leader. If such a request lands on a node that still isn't the leader (the
// forwarder's leader view was stale, or leadership is flapping), the receiver
// returns 503 instead of forwarding again — a one-hop guard against proxy
// loops. The caller retries; by then the leader view has usually settled.
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
// deadline. The caller can retry, or use leader_id to target the leader
// directly. See raft-leader-read-proxy.md.
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

		h.proxyToLeader(w, r, leaderURL, leaderID)
	}
}

// proxyToLeader forwards r to the leader's API and copies the response back
// verbatim (status, Content-Type, body).
func (h *HTTP) proxyToLeader(w httpgo.ResponseWriter, r *httpgo.Request, leaderURL string, leaderID uint64) {
	// The scheme and host are fixed by the leader's announced URL — trusted,
	// replicated state, not request input. Only the path and query come from
	// the request, joined onto the trusted base, so a crafted request can't
	// redirect the hop to an arbitrary host (no SSRF).
	base, err := url.Parse(leaderURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		writeLeaderUnavailable(w, leaderID, "the leader's announced API address is not a valid URL")
		return
	}
	base.Path = path.Join(base.Path, r.URL.Path)
	base.RawQuery = r.URL.RawQuery

	//nolint:gosec // G704: target host/scheme are the leader's replicated API URL, not request input; only path+query are forwarded.
	req, err := httpgo.NewRequestWithContext(r.Context(), r.Method, base.String(), r.Body)
	if err != nil {
		writeLeaderUnavailable(w, leaderID, "could not build a request to the leader")
		return
	}
	// Forward the headers that matter for auth, content negotiation, and
	// tracing; set the loop-guard marker so the leader (or a stale-view
	// non-leader) can tell this is a forwarded request.
	copyHeader(req.Header, r.Header, "Authorization", "Content-Type", "Accept", "X-Request-ID")
	req.Header.Set(forwardedHeader, "1")

	resp, err := h.proxyClient.Do(req) //nolint:gosec // G704: target is the trusted leader URL (see above).
	if err != nil {
		writeLeaderUnavailable(w, leaderID, "could not reach the leader")
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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
		message, map[string]any{"leader_id": leaderID})
}
