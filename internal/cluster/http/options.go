package http

// Option configures behaviour of New.
type Option func(*options)

type options struct {
	bearerToken string
}

// WithBearerToken enables bearer-token authentication on every route
// except /health. Clients must send an Authorization: Bearer <token>
// header that matches the configured value. An empty token disables
// authentication (dev mode).
func WithBearerToken(token string) Option {
	return func(o *options) { o.bearerToken = token }
}
