package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// defaultMySQLPort is used when a mysql:// URL omits the port, so a portless URL
// resolves to the same endpoint on every consumer (the database/sql DSN and the
// CDC binlog syncer) instead of the DSN path defaulting it while the binlog path
// rejected it.
const defaultMySQLPort = 3306

// MySQL TLS modes, mirroring libpq/pgx (Postgres) so a MySQL source or sink is
// secured with the same sslmode vocabulary as a Postgres one.
const (
	sslModeDisable    = "disable"     // no TLS
	sslModeRequire    = "require"     // encrypt, do not authenticate the server
	sslModeVerifyCA   = "verify-ca"   // verify the cert chain, skip the hostname
	sslModeVerifyFull = "verify-full" // verify the cert chain and the hostname
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

	// TLS, mirroring the libpq params Postgres honors through pgx, so a MySQL
	// endpoint is secured with the same vocabulary as a Postgres one. SSLMode is
	// always one of the four sslMode* constants (defaulted from the scheme:
	// mysqls:// verifies, mysql:// is plaintext). The cert fields are node-local
	// file paths, read when the *tls.Config is built (TLSClientConfig).
	SSLMode    string // sslmode
	RootCert   string // sslrootcert: custom CA PEM; "" = system roots
	ClientCert string // sslcert: client certificate PEM (mutual TLS)
	ClientKey  string // sslkey: client private key PEM
}

// ParseMySQLConn parses and validates a canonical mysql:// / mysqls:// URL. It
// requires a mysql(s) scheme (a legacy bare DSN or non-URL is rejected up front),
// defaults the port to 3306 when omitted, rejects a non-numeric or out-of-range
// port, and parses the libpq-style TLS params (sslmode / sslrootcert / sslcert /
// sslkey), defaulting sslmode from the scheme. Any other query parameter is
// rejected: MySQLConn hand-builds the driver connection, so an unrecognized param
// would be silently dropped (unlike Postgres, whose URL passes through to pgx) —
// and some go-sql-driver params (e.g. parseTime) would break committed's
// byte-parity invariants. So a portless/bad-port/TLS-misconfigured URL resolves
// identically (or is rejected) at admission and runtime. Errors are
// redaction-safe: like ParseConnString they never echo the (${VAR}-resolved)
// connection string (the port and param names are not secret).
func ParseMySQLConn(connectionString string) (MySQLConn, error) {
	u, err := ParseConnString(connectionString)
	if err != nil {
		return MySQLConn{}, err
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "mysql" && scheme != "mysqls" {
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

	q := u.Query()
	// Reject any query parameter that is not a supported TLS param — fail closed
	// rather than silently drop it.
	for k := range q {
		switch k {
		case "sslmode", "sslrootcert", "sslcert", "sslkey":
		default:
			return MySQLConn{}, fmt.Errorf("unsupported mysql connection parameter %q (supported: sslmode, sslrootcert, sslcert, sslkey)", k)
		}
	}
	sslMode := q.Get("sslmode")
	if sslMode == "" {
		sslMode = sslModeDisable
		if scheme == "mysqls" {
			sslMode = sslModeVerifyFull
		}
	}
	switch sslMode {
	case sslModeDisable, sslModeRequire, sslModeVerifyCA, sslModeVerifyFull:
	default:
		return MySQLConn{}, fmt.Errorf("unsupported sslmode %q: use disable, require, verify-ca, or verify-full", sslMode)
	}
	if scheme == "mysqls" && sslMode == sslModeDisable {
		return MySQLConn{}, errors.New("mysqls:// implies TLS but sslmode=disable was given; use mysql:// for a plaintext connection")
	}
	clientCert, clientKey := q.Get("sslcert"), q.Get("sslkey")
	if (clientCert == "") != (clientKey == "") {
		return MySQLConn{}, errors.New("sslcert and sslkey must be provided together (client-certificate auth needs both)")
	}

	password, _ := u.User.Password()
	return MySQLConn{
		Host:       u.Hostname(),
		Port:       port,
		User:       u.User.Username(),
		Password:   password,
		Database:   strings.TrimPrefix(u.Path, "/"),
		SSLMode:    sslMode,
		RootCert:   q.Get("sslrootcert"),
		ClientCert: clientCert,
		ClientKey:  clientKey,
	}, nil
}

// Addr is the host:port endpoint the driver dials (IPv6-safe).
func (c MySQLConn) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port)))
}

// TLSClientConfig builds the *tls.Config that BOTH MySQL consumers use — the
// go-sql-driver connection (via mysql.Config.TLS) and the CDC binlog syncer (via
// BinlogSyncerConfig.TLSConfig) — from the parsed sslmode and cert paths, so one
// URL yields one TLS posture on every side. It returns (nil, nil) when TLS is
// disabled. Cert files are read here (node-local paths); a missing or invalid
// file is a surfaced error, never a silent plaintext fallback.
func (c MySQLConn) TLSClientConfig() (*tls.Config, error) {
	if c.SSLMode == "" || c.SSLMode == sslModeDisable {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.ClientCert != "" {
		cert, err := tls.LoadX509KeyPair(c.ClientCert, c.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("mysql sslcert/sslkey: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if c.RootCert != "" {
		pemBytes, err := os.ReadFile(c.RootCert)
		if err != nil {
			return nil, fmt.Errorf("mysql sslrootcert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("mysql sslrootcert %q: no PEM certificates found", c.RootCert)
		}
		cfg.RootCAs = pool
	}
	switch c.SSLMode {
	case sslModeRequire:
		// Encrypt without authenticating the server (no chain, no hostname).
		cfg.InsecureSkipVerify = true //nolint:gosec // G402: sslmode=require is an explicit, documented opt-out of verification, matching libpq
	case sslModeVerifyCA:
		// Verify the chain against the CA (custom or system) but NOT the hostname.
		// Go's tls has no built-in for this, so skip its default verification and
		// authenticate the chain by hand. Use VerifyConnection, NOT
		// VerifyPeerCertificate: the latter is skipped on a resumed TLS session, so
		// a resumed handshake would bypass the check (gosec G123); VerifyConnection
		// runs on both full and resumed handshakes.
		cfg.InsecureSkipVerify = true //nolint:gosec // G402: the chain IS verified, by verifyChainNoHostname below; only the hostname check is skipped (sslmode=verify-ca)
		cfg.VerifyConnection = verifyChainNoHostname(cfg.RootCAs)
	case sslModeVerifyFull:
		// Full verification: the tls default (chain + hostname). ServerName is the
		// host so the hostname is checked against the certificate.
		cfg.ServerName = c.Host
	}
	return cfg, nil
}

// verifyChainNoHostname authenticates the server certificate chain against roots
// (nil = system roots) WITHOUT checking the hostname — the sslmode=verify-ca
// semantics, which Go's crypto/tls has no built-in flag for. It is a
// VerifyConnection callback (runs on full AND resumed handshakes), so a resumed
// session cannot skip it.
func verifyChainNoHostname(roots *x509.CertPool) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("no server certificate presented")
		}
		opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
		for _, cert := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := cs.PeerCertificates[0].Verify(opts) // no DNSName in opts → hostname not checked
		return err
	}
}
