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

	r.Post("/database", h.AddDatabase)
	r.Post("/proposal", h.AddProposal)
	r.Post("/syncable", h.AddSyncable)
	r.Post("/type", h.AddType)

	return h
}

func (h *HTTP) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, h.r)
}

func badRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Printf("%v\n", err)
}

func internalServerError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Printf("%v\n", err)
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

func createConfiguration(w http.ResponseWriter, r *http.Request) (*cluster.Configuration, error) {
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

	w.Write([]byte(id))

	return configuration, nil
}
