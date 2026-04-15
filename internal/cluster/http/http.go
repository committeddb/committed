package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/rs/cors"
	"go.uber.org/zap"
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

	// /health, /ready, /openapi.yaml, and /docs are exempt from
	// authentication — orchestrators need the first two without
	// credentials, and the spec + Swagger UI need to be discoverable
	// by new clients before they have a token.
	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)
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

func (h *HTTP) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, h.r)
}

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
	var rs []*ConfigurationResponse

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
	w.Write(bs)
}
