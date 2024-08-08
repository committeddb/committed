package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/philborlin/committed/internal/cluster"
)

type HTTP struct {
	r *chi.Mux
	c cluster.Cluster
}

func New(c cluster.Cluster) *HTTP {
	r := chi.NewRouter()
	h := &HTTP{r: r, c: c}

	r.Post("/type", h.AddType)
	r.Post("/proposal", h.AddProposal)

	return h
}

func (h *HTTP) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, h.r)
}

func badRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Printf("%v", err)
}

func internalServerError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Printf("%v", err)
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
