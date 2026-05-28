package http

// Option configures behaviour of New.
type Option func(*options)

type options struct {
	bearerToken string
	corsOrigins []string
	corsMethods []string
	corsHeaders []string
}

// WithBearerToken enables bearer-token authentication on every route
// except /health. Clients must send an Authorization: Bearer <token>
// header that matches the configured value. An empty token disables
// authentication (dev mode).
func WithBearerToken(token string) Option {
	return func(o *options) { o.bearerToken = token }
}

// WithCORS enables CORS handling for the given allowed origins. Origins
// must be explicit ("https://app.example.com") or the literal "*" to
// allow any origin; the caller (cmd/node.go) is responsible for
// validating them. An empty origins slice leaves CORS off entirely so
// no Access-Control-* headers are emitted and the browser same-origin
// policy applies unchanged.
//
// methods and headers override the request methods and headers the
// preflight will permit. Empty slices fall back to defaultCORSMethods /
// defaultCORSHeaders so an operator who only sets origins still gets a
// usable policy.
func WithCORS(origins, methods, headers []string) Option {
	return func(o *options) {
		o.corsOrigins = origins
		o.corsMethods = methods
		o.corsHeaders = headers
	}
}
