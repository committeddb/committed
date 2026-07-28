package cluster_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestParseConnString covers the contract every dialect depends on: a valid URL
// parses, and every parse failure yields an error that names the reason but
// never echoes the (already ${VAR}-resolved, password-bearing) connection string.
func TestConnStringHasInlinePassword(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want bool
	}{
		{"literal password", "postgres://user:hunter2@host:5432/db", true},
		{"literal password with specials", "mysql://cdc:S3cr3t!@10.0.0.5:3306/shop", true},
		{"var password", "postgres://user:${DB_PASSWORD}@host:5432/db", false},
		{"var password concatenated", "postgres://user:pre${DB_PASS}@host/db", false},
		{"whole string is a var", "${ORDERS_DATABASE_URL}", false},
		{"no password", "postgres://user@host:5432/db", false},
		{"no credentials", "postgres://host:5432/db", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cluster.ConnStringHasInlinePassword(tc.in))
		})
	}
}

func TestParseConnString(t *testing.T) {
	t.Run("valid parses", func(t *testing.T) {
		u, err := cluster.ParseConnString("postgres://user:pass@localhost:5432/db?sslmode=disable")
		require.NoError(t, err)
		require.Equal(t, "localhost:5432", u.Host)
		pw, _ := u.User.Password()
		require.Equal(t, "pass", pw)
	})

	const secret = "sup3rSecretPassw0rd"
	for _, tc := range []struct {
		name, in string
	}{
		{"bad escape in path", "postgres://user:" + secret + "@h:5432/db%zz"},
		{"control char", "postgres://user:" + secret + "@h:5432/d\x7fb"},
		{"bad escape mysql", "mysql://root:" + secret + "@127.0.0.1:3306/cdc%zz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cluster.ParseConnString(tc.in)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret, "parse error leaked the connection string")
			require.Contains(t, err.Error(), "invalid connection string", "error must still name the problem")
		})
	}
}

// TestParseMySQLConn pins the single MySQL URL parse authority both the DSN path
// and the CDC binlog syncer derive from: a portless URL defaults to 3306 (so the
// two sides agree instead of the DSN path defaulting while binlog rejected it), an
// explicit port is honored, and a bad port / bad scheme is rejected at parse.
func TestParseMySQLConn(t *testing.T) {
	t.Run("explicit port", func(t *testing.T) {
		c, err := cluster.ParseMySQLConn("mysql://root:secret@10.0.0.5:3307/shop")
		require.NoError(t, err)
		require.Equal(t, "10.0.0.5", c.Host)
		require.Equal(t, uint16(3307), c.Port)
		require.Equal(t, "root", c.User)
		require.Equal(t, "secret", c.Password)
		require.Equal(t, "shop", c.Database)
		require.Equal(t, "10.0.0.5:3307", c.Addr())
	})

	t.Run("portless defaults to 3306 (the unification)", func(t *testing.T) {
		c, err := cluster.ParseMySQLConn("mysql://root:secret@dbhost/shop")
		require.NoError(t, err)
		require.Equal(t, "dbhost", c.Host)
		require.Equal(t, uint16(3306), c.Port, "a portless URL must resolve to 3306, not be rejected")
		require.Equal(t, "dbhost:3306", c.Addr(),
			"Addr carries the defaulted port so both consumers target the same endpoint")
	})

	t.Run("mysql:// defaults sslmode=disable", func(t *testing.T) {
		c, err := cluster.ParseMySQLConn("mysql://root@host:3306/db")
		require.NoError(t, err)
		require.Equal(t, "disable", c.SSLMode)
	})
	t.Run("mysqls:// defaults sslmode=verify-full", func(t *testing.T) {
		c, err := cluster.ParseMySQLConn("mysqls://root@host:3306/db")
		require.NoError(t, err)
		require.Equal(t, "verify-full", c.SSLMode)
	})
	t.Run("explicit sslmode + cert paths parse", func(t *testing.T) {
		c, err := cluster.ParseMySQLConn("mysql://root@host:3306/db?sslmode=verify-ca&sslrootcert=/ca.pem")
		require.NoError(t, err)
		require.Equal(t, "verify-ca", c.SSLMode)
		require.Equal(t, "/ca.pem", c.RootCert)
	})

	for _, tc := range []struct{ name, in string }{
		{"non-numeric port", "mysql://root@host:notaport/db"},
		{"port out of range", "mysql://root@host:99999/db"},
		{"zero port", "mysql://root@host:0/db"},
		{"wrong scheme", "postgres://root@host:3306/db"},
		{"unsupported sslmode allow", "mysql://root@host:3306/db?sslmode=allow"},
		{"unsupported sslmode prefer", "mysql://root@host:3306/db?sslmode=prefer"},
		{"unknown param", "mysql://root@host:3306/db?parseTime=true"},
		{"sslcert without sslkey", "mysql://root@host:3306/db?sslmode=verify-full&sslcert=/c.pem"},
		{"mysqls with sslmode=disable", "mysqls://root@host:3306/db?sslmode=disable"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := cluster.ParseMySQLConn(tc.in)
			require.Error(t, err)
		})
	}

	t.Run("port error is redaction-safe", func(t *testing.T) {
		const secret = "sup3rSecretPassw0rd"
		_, err := cluster.ParseMySQLConn("mysql://root:" + secret + "@host:notaport/db")
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
	})
}

// writeTestCertPEM generates a self-signed cert + key pair to temp files, for the
// sslrootcert / sslcert / sslkey file-loading paths.
func writeTestCertPEM(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "committed-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath
}

// TestMySQLConn_TLSClientConfig pins the *tls.Config the driver and the binlog
// syncer both use, per sslmode — the security-critical mapping.
func TestMySQLConn_TLSClientConfig(t *testing.T) {
	t.Run("disable yields no TLS", func(t *testing.T) {
		cfg, err := cluster.MySQLConn{SSLMode: "disable"}.TLSClientConfig()
		require.NoError(t, err)
		require.Nil(t, cfg)
	})
	t.Run("require encrypts without verifying", func(t *testing.T) {
		cfg, err := cluster.MySQLConn{SSLMode: "require"}.TLSClientConfig()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.True(t, cfg.InsecureSkipVerify)
		require.Nil(t, cfg.VerifyPeerCertificate)
		require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	})
	t.Run("verify-ca verifies the chain by hand, skips hostname", func(t *testing.T) {
		cfg, err := cluster.MySQLConn{SSLMode: "verify-ca"}.TLSClientConfig()
		require.NoError(t, err)
		require.True(t, cfg.InsecureSkipVerify, "default verification is off...")
		require.NotNil(t, cfg.VerifyConnection, "...replaced by a chain-only verifier (VerifyConnection, so a resumed session can't skip it)")
		require.Nil(t, cfg.VerifyPeerCertificate, "must NOT use VerifyPeerCertificate — skipped on session resumption")
		require.Empty(t, cfg.ServerName)
	})
	t.Run("verify-full sets ServerName for full verification", func(t *testing.T) {
		cfg, err := cluster.MySQLConn{SSLMode: "verify-full", Host: "db.internal"}.TLSClientConfig()
		require.NoError(t, err)
		require.False(t, cfg.InsecureSkipVerify)
		require.Equal(t, "db.internal", cfg.ServerName)
	})
	t.Run("custom CA is loaded into RootCAs", func(t *testing.T) {
		caPath, _ := writeTestCertPEM(t)
		cfg, err := cluster.MySQLConn{SSLMode: "verify-full", Host: "h", RootCert: caPath}.TLSClientConfig()
		require.NoError(t, err)
		require.NotNil(t, cfg.RootCAs)
	})
	t.Run("client cert is loaded into Certificates", func(t *testing.T) {
		certPath, keyPath := writeTestCertPEM(t)
		cfg, err := cluster.MySQLConn{SSLMode: "verify-full", Host: "h", ClientCert: certPath, ClientKey: keyPath}.TLSClientConfig()
		require.NoError(t, err)
		require.Len(t, cfg.Certificates, 1)
	})
	t.Run("missing CA file errors", func(t *testing.T) {
		_, err := cluster.MySQLConn{SSLMode: "verify-full", RootCert: "/no/such/ca.pem"}.TLSClientConfig()
		require.Error(t, err)
	})
	t.Run("malformed client cert errors", func(t *testing.T) {
		dir := t.TempDir()
		bad := filepath.Join(dir, "bad.pem")
		require.NoError(t, os.WriteFile(bad, []byte("not a cert"), 0o600))
		_, err := cluster.MySQLConn{SSLMode: "require", ClientCert: bad, ClientKey: bad}.TLSClientConfig()
		require.Error(t, err)
	})
}
