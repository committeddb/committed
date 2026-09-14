package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A deployment still setting a renamed variable must be refused, not ignored.
// Peer mTLS is all-or-nothing and an unset trio means PLAINTEXT, so silently
// dropping the old names would turn an upgrade into a security downgrade.
func TestCheckRemovedEnvVars(t *testing.T) {
	require.NoError(t, checkRemovedEnvVars(), "a clean environment boots")

	t.Run("peer TLS", func(t *testing.T) {
		t.Setenv("COMMITTED_TLS_CA_FILE", "/etc/ca.pem")
		err := checkRemovedEnvVars()
		require.Error(t, err)
		require.Contains(t, err.Error(), "COMMITTED_PEER_TLS_CA_FILE",
			"the refusal must name the replacement")
		require.Contains(t, err.Error(), "plaintext",
			"and say why silence was not an option")
	})

	t.Run("reports every renamed variable at once", func(t *testing.T) {
		t.Setenv("COMMITTED_TLS_CERT_FILE", "/etc/c.pem")
		t.Setenv("COMMITTED_HTTP_TLS_CA_FILE", "/etc/api-ca.pem")
		err := checkRemovedEnvVars()
		require.Error(t, err)
		require.Contains(t, err.Error(), "COMMITTED_PEER_TLS_CERT_FILE")
		require.Contains(t, err.Error(), "COMMITTED_HTTP_CLIENT_TLS_CA_FILE",
			"one pass over the deployment, not one restart per variable")
	})
}
