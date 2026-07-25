package http

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/committeddb/committed/internal/cluster"
)

// syncError wraps a webhook delivery failure. Both Error() (node-local logs) and
// RedactedMessage() (the replicated dead-letter/stuck record and the
// status/errors/replay APIs) render the endpoint as scheme://host, so a
// secret-bearing webhook URL — a Slack/Discord path, a ?token= query, or userinfo —
// never reaches a log OR replicated state, matching how a DB connection error is
// redacted everywhere. The underlying *url.Error still holds the resolved secret
// URL but stays in memory only, reachable via errors.As for a caller that needs it.
// It satisfies cluster.RedactedError, which safeDeadLetterMessage honors.
type syncError struct {
	label  string // committed's classifier, e.g. "[http.Sync] request failed"
	target string // endpoint scheme://host only — safe to replicate
	err    error  // may be a *url.Error whose text carries the full secret-bearing URL
}

// Error renders the failure for node-local logs, redacting a *url.Error's URL to
// the endpoint (scheme://host) so a secret-bearing webhook URL never reaches a log.
// errors.As still unwraps to the raw *url.Error (full URL) in memory.
func (e *syncError) Error() string {
	if ue, ok := errors.AsType[*url.Error](e.err); ok {
		return fmt.Sprintf("%s: %s %q: %v", e.label, ue.Op, e.target, ue.Err)
	}
	return fmt.Sprintf("%s: %v", e.label, e.err)
}

func (e *syncError) RedactedMessage() string {
	return fmt.Sprintf("%s (endpoint %s) — see this node's logs", e.label, e.target)
}

func (e *syncError) Unwrap() error { return e.err }

var _ cluster.RedactedError = (*syncError)(nil)

// redactedTarget returns rawURL's scheme://host with no path, query, or userinfo —
// safe to replicate. It never echoes the raw string (which may carry a secret): on
// a parse failure or a hostless URL it returns a neutral placeholder.
func redactedTarget(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "unknown"
	}
	return u.Scheme + "://" + u.Host
}
