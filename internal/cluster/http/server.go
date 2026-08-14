package http

import (
	"crypto/tls"
	"net/http"
	"time"
)

// Package defaults. Callers that want something different pass the
// matching ServerOption to NewServer. Values chosen to be generous
// enough that day-one operators don't trip over them while still
// bounding pathological clients.
const (
	defaultReadHeaderTimeout = 10 * time.Second
	defaultReadTimeout       = 30 * time.Second
	// defaultWriteTimeout must EXCEED the largest bounded work a handler can
	// legitimately perform before writing its response, or the server kills
	// the connection while the handler is still (correctly) working and the
	// client receives a dead socket instead of the error — the field finding:
	// a config POST against a locked sink table was rejected loudly in the
	// log at initTimeout (60s) while curl saw an empty connection close at
	// the old 30s write deadline. Current inventory of bounded handler
	// phases: destination build (sql.InitTimeout, 60s) + propose (30s) →
	// worst case ~90s; 120s leaves margin. The relationship is pinned by
	// TestWriteTimeoutExceedsHandlerWorkBudget — change either side and the
	// test forces the arithmetic to be redone consciously.
	defaultWriteTimeout = 120 * time.Second
	defaultIdleTimeout  = 120 * time.Second
)

// ServerOption mutates the *http.Server built by NewServer. Used by
// cmd/node.go to translate env-var overrides into server timeouts
// without giving that package knowledge of the server's field layout.
type ServerOption func(*http.Server)

// WithReadHeaderTimeout overrides the default ReadHeaderTimeout.
// Zero or negative is ignored (keep the default).
func WithReadHeaderTimeout(d time.Duration) ServerOption {
	return func(s *http.Server) {
		if d > 0 {
			s.ReadHeaderTimeout = d
		}
	}
}

// WithReadTimeout overrides the default ReadTimeout.
func WithReadTimeout(d time.Duration) ServerOption {
	return func(s *http.Server) {
		if d > 0 {
			s.ReadTimeout = d
		}
	}
}

// WithWriteTimeout overrides the default WriteTimeout.
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(s *http.Server) {
		if d > 0 {
			s.WriteTimeout = d
		}
	}
}

// WithIdleTimeout overrides the default IdleTimeout.
func WithIdleTimeout(d time.Duration) ServerOption {
	return func(s *http.Server) {
		if d > 0 {
			s.IdleTimeout = d
		}
	}
}

// WithTLSConfig attaches a tls.Config to the server. The caller drives
// ListenAndServeTLS when TLSConfig != nil; nil keeps the plaintext
// default. cmd/node.go builds the config from COMMITTED_HTTP_TLS_* env
// vars so the http package stays free of filesystem I/O and credential
// handling.
func WithTLSConfig(cfg *tls.Config) ServerOption {
	return func(s *http.Server) {
		s.TLSConfig = cfg
	}
}

// NewServer builds a configured *http.Server. Callers that want graceful
// shutdown (cmd/node.go) drive ListenAndServe + Shutdown themselves; the
// ListenAndServe convenience below is kept for tests and tooling that
// just want to block until error.
//
// Timeouts bound how long a slow or malicious client can hold a
// connection. ReadHeaderTimeout prevents Slowloris. ReadTimeout and
// WriteTimeout cover full request/response, sized for the largest
// reasonable config payload (schemas, TOML bundles). IdleTimeout caps
// keepalive so dead peers don't pin file descriptors.
func (h *HTTP) NewServer(addr string, opts ...ServerOption) *http.Server {
	s := &http.Server{
		Addr:              addr,
		Handler:           h.r,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}
	for _, fn := range opts {
		fn(s)
	}
	return s
}

func (h *HTTP) ListenAndServe(addr string) error {
	return h.NewServer(addr).ListenAndServe()
}
