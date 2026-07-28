package cluster

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// defaultMySQLPort is used when a mysql:// URL omits the port, so a portless URL
// resolves to the same endpoint on every consumer (the database/sql DSN and the
// CDC binlog syncer) instead of the DSN path defaulting it while the binlog path
// rejected it.
const defaultMySQLPort = 3306

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

// MySQLConn is the parsed, validated shape of a canonical mysql:// (or mysqls://)
// URL. It is the SINGLE parse authority every MySQL consumer derives from — the
// go-sql-driver DSN (syncable sink + ingest snapshot) and the CDC binlog syncer —
// so one URL yields identical host/port/credentials on both sides and admission
// can never accept a URL the runtime then rejects. Build it with ParseMySQLConn;
// render the driver DSN with DSN().
type MySQLConn struct {
	Host     string // hostname without port or IPv6 brackets (net/url Hostname())
	Port     uint16 // the URL's explicit port, or defaultMySQLPort when omitted
	User     string
	Password string
	Database string
}

// ParseMySQLConn parses and validates a canonical mysql:// / mysqls:// URL. It
// requires a mysql(s) scheme (a legacy bare DSN or non-URL is rejected up front),
// defaults the port to 3306 when omitted, and rejects a non-numeric or
// out-of-range port — so a portless or bad-port URL resolves identically (or is
// rejected) at both admission and runtime, rather than passing the lenient DSN
// path and failing the strict binlog path. Errors are redaction-safe: like
// ParseConnString they never echo the (${VAR}-resolved) connection string (the
// port is not secret, so it may appear in a port error).
func ParseMySQLConn(connectionString string) (MySQLConn, error) {
	u, err := ParseConnString(connectionString)
	if err != nil {
		return MySQLConn{}, err
	}
	if s := strings.ToLower(u.Scheme); s != "mysql" && s != "mysqls" {
		return MySQLConn{}, errors.New("mysql connection string must be a mysql:// or mysqls:// URL")
	}
	port := uint16(defaultMySQLPort)
	// net.SplitHostPort errors only when no port is present (missing port) — a
	// portless URL keeps the default. A present port section is validated as
	// numeric and in range (SplitHostPort itself does not check numericness).
	if _, portStr, splitErr := net.SplitHostPort(u.Host); splitErr == nil {
		n, perr := strconv.ParseUint(portStr, 10, 16)
		if perr != nil || n == 0 {
			return MySQLConn{}, fmt.Errorf("invalid mysql connection port %q: must be 1-65535", portStr)
		}
		port = uint16(n)
	}
	password, _ := u.User.Password()
	return MySQLConn{
		Host:     u.Hostname(),
		Port:     port,
		User:     u.User.Username(),
		Password: password,
		Database: strings.TrimPrefix(u.Path, "/"),
	}, nil
}

// DSN renders the go-sql-driver/mysql DSN ("user:pw@tcp(host:port)/db") the
// database/sql consumers (syncable sink + ingest snapshot) Open with. The port is
// always explicit (defaulted by ParseMySQLConn), so the DSN and the binlog syncer
// target the same endpoint.
func (c MySQLConn) DSN() string {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port)))
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", c.User, c.Password, addr, c.Database)
}

// MySQLDSN is a convenience wrapper: ParseMySQLConn(connectionString).DSN(). It
// is kept for the call sites that only need the driver DSN string.
func MySQLDSN(connectionString string) (string, error) {
	c, err := ParseMySQLConn(connectionString)
	if err != nil {
		return "", err
	}
	return c.DSN(), nil
}
