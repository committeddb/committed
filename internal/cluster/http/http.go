package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/oklog/ulid/v2"
	"github.com/philborlin/committed/internal/cluster"
)

type HTTP struct {
	r *chi.Mux
	c cluster.Cluster
}

func New(c cluster.Cluster) *HTTP {
	r := chi.NewRouter()
	h := &HTTP{r: r, c: c}

	r.Get("/database", h.GetDatabases)
	r.Post("/database", h.AddDatabase)

	r.Get("/ingestable", h.GetIngestables)
	r.Post("/ingestable", h.AddIngestable)

	r.Post("/proposal", h.AddProposal)

	r.Get("/syncable", h.GetSyncables)
	r.Post("/syncable", h.AddSyncable)

	r.Get("/type", h.GetTypes)
	r.Post("/type", h.AddType)

	return h
}

func (h *HTTP) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, h.r)
}

func badRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Printf("[http] %v\n", err)
}

func internalServerError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Printf("[http] %v\n", err)
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
	mimeType := "text/toml"
	header, ok := r.Header["Content-Type"]
	if ok && len(header) == 1 {
		mimeType = header[0]
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	id := fmt.Sprintf("%v", ulid.Make())
	configuration := &cluster.Configuration{
		ID:       string(id),
		MimeType: mimeType,
		Data:     body,
	}

	return configuration, nil
}

type ConfigurationResponse struct {
	ID string `json:"id"`
}

func writeConfigurations(w http.ResponseWriter, cfgs []*cluster.Configuration) {
	var rs []*ConfigurationResponse

	for _, cfg := range cfgs {
		rs = append(rs, &ConfigurationResponse{ID: cfg.ID})
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
		internalServerError(w, err)
	}

	writeJson(w, bs)
}

func writeJson(w http.ResponseWriter, bs []byte) {
	w.Header().Add("Content-Type", "application/json")
	w.Write(bs)
}
