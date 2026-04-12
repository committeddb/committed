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
	schemas     sync.Map // type ID → *jsonschema.Schema
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

	// /health and /ready are exempt from authentication — orchestrators
	// use them as liveness/readiness probes without credentials.
	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)

	r.Group(func(r chi.Router) {
		if h.bearerToken != "" {
			r.Use(h.bearerAuth)
		}

		r.Get("/database", h.GetDatabases)
		r.Post("/database/{id}", h.AddDatabase)

		r.Get("/ingestable", h.GetIngestables)
		r.Post("/ingestable/{id}", h.AddIngestable)

		r.Get("/proposal", h.GetProposals)
		r.Post("/proposal", h.AddProposal)

		r.Get("/syncable", h.GetSyncables)
		r.Post("/syncable/{id}", h.AddSyncable)

		r.Get("/type", h.GetTypes)
		r.Get("/type/{id}", h.GetType)
		r.Post("/type/{id}", h.AddType)
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
