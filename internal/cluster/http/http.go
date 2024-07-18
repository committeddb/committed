package http

import (
	httpgo "net/http"

	"github.com/go-chi/chi/v5"
)

type HTTP struct {
	r *chi.Mux
}

func New() *HTTP {
	r := chi.NewRouter()
	h := &HTTP{r: r}

	r.Post("/type", h.AddType)
	r.Post("/proposal", h.AddProposal)

	return h
}

func (h *HTTP) ListenAndServe(addr string) {
	httpgo.ListenAndServe(addr, h.r)
}
