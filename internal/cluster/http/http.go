package http

import (
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
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

// defaultMaxBodyBytes is the default request-body cap: 2x the 16 MiB default
// proposal size (db.DefaultMaxProposalBytes), giving headroom for the JSON/TOML
// body being larger than the marshaled proposal it produces. cmd overrides it
// via WithMaxBodyBytes to track a raised proposal cap.
const defaultMaxBodyBytes int64 = 32 * 1024 * 1024

// maxBytes wraps each request body in http.MaxBytesReader so an over-limit body
// fails the read (with *http.MaxBytesError → 413) instead of being buffered into
// memory — an OOM-DoS guard. The propose layer still enforces the exact
// marshaled proposal size; this only bounds what a node will read into RAM.
func maxBytes(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limit > 0 && r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// routePattern returns the matched chi route pattern (e.g. "/v1/proposal") for
// logs and metrics, falling back to the raw path. The pattern is bounded
// cardinality (path params collapse to "{id}"), so it is safe as a metric label.
func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		if p := rc.RoutePattern(); p != "" {
			return p
		}
	}
	return r.URL.Path
}

type HTTP struct {
	r                *chi.Mux
	c                cluster.Cluster
	schemas          sync.Map // schemaCacheKey → *jsonschema.Schema
	bearerToken      string   // empty = no auth (dev mode)
	readIndexTimeout time.Duration
	// proxyClient performs the follower→leader hop for leaderRead-wrapped
	// routes (GET /v1/membership). Defaults to a timeout-bounded client with
	// system-root TLS; cmd/node.go overrides it via WithProxyClient to trust
	// the cluster's CA or skip verification for self-signed peer certs.
	proxyClient *http.Client
	// metrics is nil when instrumentation is disabled; call sites nil-check.
	metrics *metrics.Metrics
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
	// After RequestID so the panic log carries the request ID; before
	// bearerAuth and every handler so the net covers the whole surface.
	r.Use(recoverPanic)

	maxBody := o.maxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	// Bound every request body so a large upload can't OOM the node before the
	// propose-layer size check runs. An over-cap body fails the read with 413.
	r.Use(maxBytes(maxBody))

	readIndexTimeout := o.readIndexTimeout
	if readIndexTimeout <= 0 {
		readIndexTimeout = defaultReadIndexTimeout
	}

	proxyClient := o.proxyClient
	if proxyClient == nil {
		proxyClient = &http.Client{Timeout: defaultProxyTimeout}
	}

	h := &HTTP{r: r, c: c, bearerToken: o.bearerToken, readIndexTimeout: readIndexTimeout, proxyClient: proxyClient, metrics: o.metrics}

	if o.bearerToken != "" {
		zap.L().Info("API bearer-token authentication enabled")
	} else {
		zap.L().Warn("API authentication disabled (no COMMITTED_API_TOKEN set)")
	}

	// Router-level error paths honor the JSON error-envelope contract instead of
	// chi's raw text/plain 404 / empty-body 405. The /v1 subrouter sets its own
	// NotFound below so an unmatched /v1 path is answered from INSIDE the auth
	// group (authenticated) rather than leaking a 404 to an unauthenticated caller.
	r.NotFound(notFoundHandler)
	r.MethodNotAllowed(methodNotAllowedHandler)

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
		// per-resource deviations — type has no rollback and adds
		// migration-errors/migration-retry, syncable adds
		// errors/status/deadletter/replay — remain visible rather than
		// hidden inside a registrar.
		r.Route("/v1", func(r chi.Router) {
			r.Get("/database", h.listConfig("database", h.c.Databases))
			r.Post("/database/{id}", h.addConfig("database", h.c.ProposeDatabase, h.c.DatabaseVersions))
			r.Get("/database/{id}/versions", h.getVersions("database", h.c.DatabaseVersions))
			r.Get("/database/{id}/versions/{version}", h.getVersion("database", h.c.DatabaseVersion))
			r.Post("/database/{id}/rollback", h.rollback("database", h.c.DatabaseVersion, h.c.ProposeDatabase, h.c.DatabaseVersions))

			r.Get("/ingestable", h.listConfig("ingestable", h.c.Ingestables))
			r.Post("/ingestable/{id}", h.AddIngestable)
			r.Get("/ingestable/{id}/versions", h.getVersions("ingestable", h.c.IngestableVersions))
			r.Get("/ingestable/{id}/versions/{version}", h.getVersion("ingestable", h.c.IngestableVersion))
			r.Get("/ingestable/{id}/status", h.GetIngestableStatus)
			r.Post("/ingestable/{id}/rollback", h.rollback("ingestable", h.c.IngestableVersion, h.c.ProposeIngestable, h.c.IngestableVersions))
			// DELETE is leader-pinned (leaderRead reverse-proxies a follower's
			// request to the leader): the owner-gated source teardown — dropping the
			// Postgres replication slot + publication — runs on the leader, so the
			// request must land where the teardown does.
			r.Delete("/ingestable/{id}", h.leaderRead(h.DeleteIngestable))

			// /proposal is write-only by design: Committed is a commit log,
			// not a query interface. Reads happen on the synced query side
			// (hook up a syncable), not over HTTP. See the API overview in
			// api/openapi.yaml.
			r.Post("/proposal", h.AddProposal)

			// /scrub is the manual right-to-be-forgotten lever: physically
			// remove already-delete-proposed entities from the permanent event
			// log up to the current applied index. Authenticated like every
			// other write; the automatic scheduler does the same on a cadence.
			r.Post("/scrub", h.Scrub)

			r.Get("/syncable", h.listConfig("syncable", h.c.Syncables))
			r.Post("/syncable/{id}", h.addConfig("syncable", h.c.ProposeSyncable, h.c.SyncableVersions))
			// DELETE is leader-pinned (leaderRead reverse-proxies a follower's
			// request to the leader): the owner-gated destination teardown runs on
			// the leader and honors the keepData flag recorded there, so the
			// request that carries keepData must land on the same node that tears
			// down.
			r.Delete("/syncable/{id}", h.leaderRead(h.DeleteSyncable))
			r.Get("/syncable/{id}/versions", h.getVersions("syncable", h.c.SyncableVersions))
			r.Get("/syncable/{id}/versions/{version}", h.getVersion("syncable", h.c.SyncableVersion))
			r.Get("/syncable/{id}/errors", h.GetSyncableErrors)
			r.Get("/syncable/{id}/status", h.GetSyncableStatus)
			r.Post("/syncable/{id}/deadletter", h.DeadLetterStuckSyncable)
			r.Post("/syncable/{id}/replay/{index}", h.ReplaySyncableDeadLetter)
			r.Post("/syncable/{id}/rollback", h.rollback("syncable", h.c.SyncableVersion, h.c.ProposeSyncable, h.c.SyncableVersions))
			// Rebuild is leader-pinned for the same reason as DELETE: the
			// owner-side destination teardown/re-init runs on the leader.
			r.Post("/syncable/{id}/rebuild", h.leaderRead(h.RebuildSyncable))

			r.Get("/type", h.listConfig("type", h.c.Types))
			r.Post("/type/{id}", h.addConfig("type", h.c.ProposeType, h.c.TypeVersions))
			r.Get("/type/{id}/versions", h.getVersions("type", h.c.TypeVersions))
			r.Get("/type/{id}/versions/{version}", h.getVersion("type", h.c.TypeVersion))
			r.Get("/type/{id}/migration-errors", h.GetTypeMigrationErrors)
			r.Post("/type/{id}/migration-retry/{index}", h.ReplayTypeMigrationDeadLetter)
			r.Get("/type/{id}/pipeline", h.GetPipelineStatus)

			// Per-node diagnostics. /node/ (not /status) scopes this to the
			// answering node — degraded-build state is node-local and
			// ephemeral, unlike the replicated config content — and reserves
			// /cluster/status for a future fan-out sibling.
			r.Get("/node/status", h.NodeStatus)

			// Node-to-node: members push their disk state to the leader
			// here and take the cluster write-admission verdict home from
			// the response — the transport for cluster-aware disk
			// admission (see db/disk_cluster.go). Authenticated with the
			// same bearer token as every other write, which is why the
			// token must be cluster-uniform.
			r.Post("/node/disk-report", h.DiskReport)

			// Live cluster membership. GET lists members with their roles
			// and (leader-observed) replication progress; POST adds a voter
			// (or a learner with "learner": true); POST .../promote promotes
			// a learner to a voter; DELETE removes a node — all via
			// joint-consensus (ConfChangeV2) raft reconfiguration.
			// Authenticated like every other write. GET is leaderRead-wrapped
			// so a follower proxies to the leader, giving a load-balanced
			// caller a leader-truthful answer (per-member match index is
			// leader-only state). The node added via POST must first be
			// started in join mode. See docs/operations/membership.md.
			r.Get("/membership", h.leaderRead(h.GetMembership))
			r.Post("/membership", h.AddMember)
			r.Post("/membership/{id}/promote", h.PromoteMember)
			r.Delete("/membership/{id}", h.RemoveMember)

			// Answer unmatched /v1 paths from inside this (authenticated)
			// subrouter, so a probe for a nonexistent /v1 route requires the
			// bearer token and returns the JSON envelope — not chi's
			// unauthenticated default 404 outside the auth group.
			r.NotFound(notFoundHandler)
			r.MethodNotAllowed(methodNotAllowedHandler)
		})
	})

	return h
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.r.ServeHTTP(w, r)
}
