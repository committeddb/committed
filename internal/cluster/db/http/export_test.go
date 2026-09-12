package http

import "github.com/go-chi/chi/v5"

// RouterForTest exposes the mounted chi router so the OpenAPI drift guard (in
// package http_test) can walk the live route table via chi.Walk, instead of
// checking the spec against a hand-maintained list that silently rots out of
// date. Test-only.
func (h *HTTP) RouterForTest() chi.Routes { return h.r }
