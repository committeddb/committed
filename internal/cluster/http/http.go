package http

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

// Default CORS request methods and headers, applied when WithCORS is
// given origins but no explicit methods/headers. These cover every verb
// the API actually uses plus the headers a browser client sends on a
// configured request (Content-Type for JSON/TOML bodies, Authorization
// for the bearer token, X-Request-ID for trace correlation).
var (
	defaultCORSMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	defaultCORSHeaders = []string{"Content-Type", "Authorization", "X-Request-ID"}
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

	// CORS is off unless an operator supplies an allowlist via
	// WithCORS (COMMITTED_HTTP_CORS_ORIGINS). With no allowlist we mount
	// no CORS middleware at all, so no Access-Control-* headers are
	// emitted and the browser's same-origin policy decides — the right
	// default for a server binary with no in-tree browser client.
	if len(o.corsOrigins) > 0 {
		methods := o.corsMethods
		if len(methods) == 0 {
			methods = defaultCORSMethods
		}
		headers := o.corsHeaders
		if len(headers) == 0 {
			headers = defaultCORSHeaders
		}
		corsMiddleware := cors.New(cors.Options{
			AllowedOrigins: o.corsOrigins,
			AllowedMethods: methods,
			AllowedHeaders: headers,
		})
		r.Use(corsMiddleware.Handler)
	}

	r.Use(securityHeaders)
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

		// Every API endpoint lives under /v1 so a future breaking change
		// can bump the prefix instead of breaking existing clients. See
		// docs/api-compatibility.md for the versioning contract.
		//
		// The four config resources share a generic handler set (see
		// config_handlers.go); the route table stays explicit so
		// per-resource deviations — type has no rollback and a bespoke
		// GET /type/{id}, syncable adds errors/status/deadletter/replay —
		// remain visible rather than hidden inside a registrar.
		r.Route("/v1", func(r chi.Router) {
			r.Get("/database", h.listConfig("database", h.c.Databases))
			r.Post("/database/{id}", h.addConfig("database", h.c.ProposeDatabase))
			r.Get("/database/{id}/versions", h.getVersions("database", h.c.DatabaseVersions))
			r.Get("/database/{id}/versions/{version}", h.getVersion("database", h.c.DatabaseVersion))
			r.Post("/database/{id}/rollback", h.rollback("database", h.c.DatabaseVersion, h.c.ProposeDatabase))

			r.Get("/ingestable", h.listConfig("ingestable", h.c.Ingestables))
			r.Post("/ingestable/{id}", h.addConfig("ingestable", h.c.ProposeIngestable))
			r.Get("/ingestable/{id}/versions", h.getVersions("ingestable", h.c.IngestableVersions))
			r.Get("/ingestable/{id}/versions/{version}", h.getVersion("ingestable", h.c.IngestableVersion))
			r.Post("/ingestable/{id}/rollback", h.rollback("ingestable", h.c.IngestableVersion, h.c.ProposeIngestable))

			// /proposal is write-only by design: Committed is a commit log,
			// not a query interface. Reads happen on the synced query side
			// (hook up a syncable), not over HTTP. See the API overview in
			// api/openapi.yaml.
			r.Post("/proposal", h.AddProposal)

			r.Get("/syncable", h.listConfig("syncable", h.c.Syncables))
			r.Post("/syncable/{id}", h.addConfig("syncable", h.c.ProposeSyncable))
			r.Get("/syncable/{id}/versions", h.getVersions("syncable", h.c.SyncableVersions))
			r.Get("/syncable/{id}/versions/{version}", h.getVersion("syncable", h.c.SyncableVersion))
			r.Get("/syncable/{id}/errors", h.GetSyncableErrors)
			r.Get("/syncable/{id}/status", h.GetSyncableStatus)
			r.Post("/syncable/{id}/deadletter/", h.DeadLetterStuckSyncable)
			r.Post("/syncable/{id}/replay/{index}", h.ReplaySyncableDeadLetter)
			r.Post("/syncable/{id}/rollback", h.rollback("syncable", h.c.SyncableVersion, h.c.ProposeSyncable))

			r.Get("/type", h.listConfig("type", h.c.Types))
			r.Post("/type/{id}", h.addConfig("type", h.c.ProposeType))
			r.Get("/type/{id}/versions", h.getVersions("type", h.c.TypeVersions))
			r.Get("/type/{id}/versions/{version}", h.getVersion("type", h.c.TypeVersion))

			// Per-node diagnostics. /node/ (not /status) scopes this to the
			// answering node — degraded-build state is node-local and
			// ephemeral, unlike the replicated config content — and reserves
			// /cluster/status for a future fan-out sibling.
			r.Get("/node/status", h.NodeStatus)
		})
	})

	return h
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.r.ServeHTTP(w, r)
}
