package cluster

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ParseConnString parses a database connection-string URL, returning an error
// that NEVER echoes the connection string itself. Every dialect — ingest AND
// syncable/database — must use this rather than url.Parse (or a bare driver
// Open) on a Config.ConnectionString, so the redaction lives in exactly one
// place and cannot drift between the two sides.
//
// This matters because the connection string is already ${VAR}-interpolated —
// RESOLVED to the real plaintext password — by the time a dialect parses it (the
// parser interpolates at the config boundary). url.Parse wraps every failure in
// *url.Error, whose Error() and .URL field embed the raw input verbatim. So
// returning or %w-wrapping that error leaks the password into an HTTP 400 body
// (ingestable-config parse), into ingest-runtime logs, and into the node-status /
// pipeline surfaces the syncable side reports — defeating the whole point of
// keeping secrets out of the log via ${VAR}.
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

// MySQLDSN converts a canonical mysql:// connection URL into the
// go-sql-driver/mysql DSN its Open expects ("user:pw@tcp(host:port)/db"). It is
// the single URL→DSN conversion shared by the ingest snapshot connection and the
// syncable sink dialect, so one URL yields the same DSN regardless of which side
// opens it — committed's connection strings are canonically URLs everywhere, and
// only this seam knows the driver's native shape.
//
// It requires a mysql (or mysqls) scheme so a legacy bare DSN or a non-URL is
// rejected up front rather than silently mis-parsed. Errors are redaction-safe:
// like ParseConnString they never echo the (${VAR}-resolved) connection string.
func MySQLDSN(connectionString string) (string, error) {
	u, err := ParseConnString(connectionString)
	if err != nil {
		return "", err
	}
	if s := strings.ToLower(u.Scheme); s != "mysql" && s != "mysqls" {
		return "", errors.New("mysql connection string must be a mysql:// URL")
	}
	username := u.User.Username()
	password, _ := u.User.Password()
	database := strings.TrimPrefix(u.Path, "/")
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, u.Host, database), nil
}
