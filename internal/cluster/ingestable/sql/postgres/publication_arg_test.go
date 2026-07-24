package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A config-controlled publication name sits in a '...' SQL string literal in the
// START_REPLICATION plugin args; a quote-bearing name (creatable via a
// double-quoted identifier) must be escaped so it stays inside the literal
// rather than breaking out.
func TestPublicationNamesArg_EscapesQuote(t *testing.T) {
	require.Equal(t, "publication_names 'committed_pub'", publicationNamesArg("committed_pub"))
	require.Equal(t, "publication_names 'a''b'", publicationNamesArg("a'b"))

	// The injection attempt — a name crafted to close the literal and toggle a
	// second replication option — stays inside the literal as doubled quotes.
	require.Equal(t, "publication_names 'p'', binary ''on'", publicationNamesArg("p', binary 'on"))
}
