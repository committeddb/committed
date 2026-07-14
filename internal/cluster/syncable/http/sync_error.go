package http

import (
	"fmt"
	"net/url"

	"github.com/committeddb/committed/internal/cluster"
)

// syncError wraps a webhook delivery failure. Its Error() carries the full detail
// — including the *url.Error from net/http, whose text embeds the RESOLVED request
// URL (path and query, where Slack/Discord/?token= webhooks put their secret) — for
// node-local logs and errors.As. RedactedMessage() reports only committed's
// classifier plus the endpoint's scheme://host (no path/query/userinfo), so a
// secret-in-URL webhook doesn't leak into the replicated dead-letter/stuck record
// or the status/errors/replay APIs. It satisfies cluster.RedactedError, which
// safeDeadLetterMessage honors: the classifier is replicated, the detail stays local.
type syncError struct {
	label  string // committed's classifier, e.g. "[http.Sync] request failed"
	target string // endpoint scheme://host only — safe to replicate
	err    error  // may be a *url.Error whose text carries the full secret-bearing URL
}

func (e *syncError) Error() string { return fmt.Sprintf("%s: %v", e.label, e.err) }

func (e *syncError) RedactedMessage() string {
	return fmt.Sprintf("%s (endpoint %s) — full detail in this node's logs", e.label, e.target)
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
