package http

import (
	httpgo "net/http"
	"time"
)

// Option configures behaviour of New.
type Option func(*options)

type options struct {
	bearerToken      string
	corsOrigins      []string
	corsMethods      []string
	corsHeaders      []string
	readIndexTimeout time.Duration
	proxyClient      *httpgo.Client
}

// WithBearerToken enables bearer-token authentication on every route
// except /health. Clients must send an Authorization: Bearer <token>
// header that matches the configured value. An empty token disables
// authentication (dev mode).
func WithBearerToken(token string) Option {
	return func(o *options) { o.bearerToken = token }
}

// WithCORS enables CORS handling for the given allowed origins. Origins
// must be explicit ("https://app.example.com") or the literal "*" to
// allow any origin; the caller (cmd/node.go) is responsible for
// validating them. An empty origins slice leaves CORS off entirely so
// no Access-Control-* headers are emitted and the browser same-origin
// policy applies unchanged.
//
// methods and headers override the request methods and headers the
// preflight will permit. Empty slices fall back to defaultCORSMethods /
// defaultCORSHeaders so an operator who only sets origins still gets a
// usable policy.
func WithCORS(origins, methods, headers []string) Option {
	return func(o *options) {
		o.corsOrigins = origins
		o.corsMethods = methods
		o.corsHeaders = headers
	}
}

// WithReadIndexTimeout bounds how long a default (linearizable) GET waits for
// the raft ReadIndex quorum confirmation before returning 503. Keeps a
// partitioned node from holding the connection open until the server
// WriteTimeout. Zero or negative keeps the package default
// (defaultReadIndexTimeout). cmd/node.go can wire this from an env var.
func WithReadIndexTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.readIndexTimeout = d
		}
	}
}

// WithProxyClient overrides the HTTP client used to proxy leader-only reads
// (GET /v1/membership) from a follower to the leader. The default client uses
// system-root TLS with defaultProxyTimeout; cmd/node.go passes a client
// configured to trust the cluster's CA (or to skip verification for
// self-signed peer certs) when the API serves TLS. A nil client keeps the
// default.
func WithProxyClient(c *httpgo.Client) Option {
	return func(o *options) {
		if c != nil {
			o.proxyClient = c
		}
	}
}
