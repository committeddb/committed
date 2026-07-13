package sql

import (
	"errors"
	"fmt"
	"net/url"
)

// ParseConnString parses a database connection-string URL, returning an error
// that NEVER echoes the connection string itself. Dialects must use this rather
// than url.Parse directly on a Config.ConnectionString.
//
// This matters because the connection string is already ${VAR}-interpolated —
// RESOLVED to the real plaintext password — by the time a dialect parses it (the
// parser interpolates at the config boundary). url.Parse wraps every failure in
// *url.Error, whose Error() and .URL field embed the raw input verbatim. So
// returning or %w-wrapping that error leaks the password into an HTTP 400 body
// (ingestable-config parse) and into ingest-runtime logs — defeating the whole
// point of keeping secrets out of the log via ${VAR}.
//
// The underlying reason (*url.Error.Err — e.g. `invalid URL escape "%zz"`,
// `net/url: invalid control character in URL`) names the problem WITHOUT the
// value, so surfacing it keeps the error actionable while dropping the secret.
func ParseConnString(connectionString string) (*url.URL, error) {
	u, err := url.Parse(connectionString)
	if err != nil {
		var uerr *url.Error
		if errors.As(err, &uerr) && uerr.Err != nil {
			// uerr.Err is the reason only; uerr (and its .URL) carries the
			// secret-bearing string, so wrap uerr.Err, never uerr.
			return nil, fmt.Errorf("invalid connection string: %w", uerr.Err)
		}
		return nil, errors.New("invalid connection string")
	}
	return u, nil
}
