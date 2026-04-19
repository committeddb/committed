package http

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

// Package defaults. Callers that want something different pass the
// matching ServerOption to NewServer. Values chosen to be generous
// enough that day-one operators don't trip over them while still
// bounding pathological clients.
const (
	defaultReadHeaderTimeout = 10 * time.Second
	defaultReadTimeout       = 30 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 120 * time.Second
)

type HTTP struct {
	r           *chi.Mux
	c           cluster.Cluster
	schemas     sync.Map // schemaCacheKey → *jsonschema.Schema
	bearerToken string   // empty = no auth (dev mode)
}

func New(c cluster.Cluster, opts ...Option) *HTTP {
	var o options
	for _, fn := range opts {
		fn(&o)
	}

	r := chi.NewRouter()

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins: []string{"http://localhost:5173", "http://localhost:4173"},
	})

	r.Use(corsMiddleware.Handler)
	r.Use(RequestID)

	h := &HTTP{r: r, c: c, bearerToken: o.bearerToken}

	if o.bearerToken != "" {
		zap.L().Info("API bearer-token authentication enabled")
	} else {
		zap.L().Warn("API authentication disabled (no COMMITTED_API_TOKEN set)")
	}

	// /health, /ready, /version, /openapi.yaml, and /docs are exempt
	// from authentication — orchestrators need the first two without
	// credentials, /version is useful during rolling upgrades before
	// an operator has a token handy, and the spec + Swagger UI need
	// to be discoverable by new clients before they have a token.
	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)
	r.Get("/version", h.Version)
	r.Get("/openapi.yaml", h.OpenAPISpec)
	r.Get("/docs", h.SwaggerUI)

	r.Group(func(r chi.Router) {
		if h.bearerToken != "" {
			r.Use(h.bearerAuth)
		}

		r.Get("/database", h.GetDatabases)
		r.Post("/database/{id}", h.AddDatabase)
		r.Get("/database/{id}/versions", h.GetDatabaseVersions)
		r.Get("/database/{id}/versions/{version}", h.GetDatabaseVersion)
		r.Post("/database/{id}/rollback", h.RollbackDatabase)

		r.Get("/ingestable", h.GetIngestables)
		r.Post("/ingestable/{id}", h.AddIngestable)
		r.Get("/ingestable/{id}/versions", h.GetIngestableVersions)
		r.Get("/ingestable/{id}/versions/{version}", h.GetIngestableVersion)
		r.Post("/ingestable/{id}/rollback", h.RollbackIngestable)

		r.Get("/proposal", h.GetProposals)
		r.Post("/proposal", h.AddProposal)

		r.Get("/syncable", h.GetSyncables)
		r.Post("/syncable/{id}", h.AddSyncable)
		r.Get("/syncable/{id}/versions", h.GetSyncableVersions)
		r.Get("/syncable/{id}/versions/{version}", h.GetSyncableVersion)
		r.Post("/syncable/{id}/rollback", h.RollbackSyncable)

		r.Get("/type", h.GetTypes)
		r.Get("/type/{id}", h.GetType)
		r.Post("/type/{id}", h.AddType)
		r.Get("/type/{id}/versions", h.GetTypeVersions)
		r.Get("/type/{id}/versions/{version}", h.GetTypeVersion)
	})

	return h
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.r.ServeHTTP(w, r)
}

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

// unmarshalBody reads r.Body and JSON-decodes into v. Oversize
// bodies are not rejected here — the proposal-size limit lives at
// the raft choke point (db.proposeAsync) so every proposal source
// is checked uniformly. See cluster.ErrProposalTooLarge.
func unmarshalBody(r *http.Request, v any) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

func createConfiguration(r *http.Request) (*cluster.Configuration, error) {
	id := r.PathValue("id")
	if id == "" {
		return nil, errors.New("id is empty")
	}

	mimeType := "text/toml"
	header, ok := r.Header["Content-Type"]
	if ok && len(header) == 1 {
		mimeType = header[0]
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	configuration := &cluster.Configuration{
		ID:       string(id),
		MimeType: mimeType,
		Data:     body,
	}

	return configuration, nil
}

type ConfigurationResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

func writeConfigurations(w http.ResponseWriter, cfgs []*cluster.Configuration) {
	rs := make([]*ConfigurationResponse, 0, len(cfgs))

	for _, cfg := range cfgs {
		rs = append(rs, &ConfigurationResponse{
			ID:       cfg.ID,
			Name:     "", // TODO - get name configuration
			MimeType: cfg.MimeType,
			Data:     string(cfg.Data),
		})
	}

	writeArrayBody(w, rs)
}

func writeArrayBody[T any](w http.ResponseWriter, body []T) {
	if body == nil {
		writeJson(w, []byte("[]"))
		return
	}

	bs, err := json.Marshal(body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to marshal response")
		return
	}

	writeJson(w, bs)
}

func writeJson(w http.ResponseWriter, bs []byte) {
	w.Header().Add("Content-Type", "application/json")
	_, _ = w.Write(bs)
}
