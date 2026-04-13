package http

import (
	httpgo "net/http"

	"github.com/philborlin/committed/api"
)

// OpenAPISpec serves the embedded OpenAPI specification as YAML. It is
// unauthenticated so clients can discover the API without credentials.
func (h *HTTP) OpenAPISpec(w httpgo.ResponseWriter, r *httpgo.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.Write(api.OpenAPISpec)
}

// SwaggerUI serves a small HTML page that loads Swagger UI from a CDN
// and renders the spec at /openapi.yaml. Unauthenticated for the same
// reason as the spec itself.
func (h *HTTP) SwaggerUI(w httpgo.ResponseWriter, r *httpgo.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(api.SwaggerUIHTML)
}
