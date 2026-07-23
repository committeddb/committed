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

// ConnStringHasInlinePassword reports whether raw — a PRE-interpolation connection
// string, i.e. one that still carries any ${VAR} references — embeds a literal
// password in its userinfo instead of referencing one via ${VAR}. It is the
// propose-time gate that keeps a plaintext credential out of the cluster's durable,
// replicated, API-readable state: committed stores configs pre-interpolation, so a
// literal user:password@ here would be written verbatim into the raft log, bbolt,
// and every snapshot, and handed back by GET /database/{id}.
//
// A ${VAR} in the password position makes the string unparseable as a URL, so
// url.Parse fails and we report false (accept) — which is exactly how the operator
// is meant to externalize the secret. No password, a whole-string ${VAR}, or any
// non-URL likewise report false. A literal password that fails to parse for some
// other reason also reports false, but such a string fails the real ParseConnString
// downstream anyway, so the config never runs.
//
// NEVER pass raw into an error message — it holds the secret; the caller's
// rejection must be value-free.
func ConnStringHasInlinePassword(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return false
	}
	pw, ok := u.User.Password()
	if !ok || pw == "" {
		return false
	}
	// Belt-and-suspenders: a ${VAR} in the password normally fails url.Parse above,
	// but if it ever parses leniently, a var reference is still the operator
	// externalizing the secret, not an inline literal.
	return !strings.Contains(pw, "${")
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
