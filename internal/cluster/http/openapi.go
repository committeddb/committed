package http

import (
	httpgo "net/http"

	"github.com/philborlin/committed/api"
)

// OpenAPISpec serves the embedded OpenAPI specification as YAML. It is
// unauthenticated so clients can discover the API without credentials.
func (h *HTTP) OpenAPISpec(w httpgo.ResponseWriter, r *httpgo.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(api.OpenAPISpec)
}

// SwaggerUI serves a small HTML page that loads Swagger UI from a CDN
// and renders the spec at /openapi.yaml. Unauthenticated for the same
// reason as the spec itself.
//
// The strict default CSP set by the securityHeaders middleware
// (default-src 'none') would block the CDN-hosted script and stylesheet
// this page needs, so we override it with a narrower allowlist scoped
// to unpkg.com. 'unsafe-inline' is unavoidable today because the HTML
// carries inline <script>/<style> blocks and Swagger UI's bundle
// injects inline styles at runtime. TODO: self-host the Swagger UI
// assets and drop the CDN + 'unsafe-inline' exceptions.
func (h *HTTP) SwaggerUI(w httpgo.ResponseWriter, r *httpgo.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; "+
			"script-src 'self' https://unpkg.com 'unsafe-inline'; "+
			"style-src 'self' https://unpkg.com 'unsafe-inline'; "+
			"img-src 'self' data:; "+
			"font-src 'self' data:; "+
			"connect-src 'self'; "+
			"frame-ancestors 'none'")
	_, _ = w.Write(api.SwaggerUIHTML)
}
