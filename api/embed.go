// Package api embeds the OpenAPI specification and the Swagger UI
// loader page so they can be served directly from the committed binary.
package api

import _ "embed"

// OpenAPISpec is the raw bytes of api/openapi.yaml. Served unchanged by
// the HTTP layer at GET /openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPISpec []byte

// SwaggerUIHTML is a small HTML page that loads Swagger UI from a CDN
// and points it at /openapi.yaml. Served by the HTTP layer at GET /docs.
//
//go:embed swagger-ui.html
var SwaggerUIHTML []byte
