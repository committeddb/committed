package migration_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/migration"
)

// TestError_RedactedMessage is the migration-PII regression: a gojq runtime error
// inlines the entity's field values, so the message replicated into the permanent
// dead-letter log (and served over HTTP) must carry only the classifier — the
// type id and failing chain step — never the underlying error text.
func TestError_RedactedMessage(t *testing.T) {
	// A gojq-style error that inlines a field value, exactly as the real ones do
	// (e.g. `cannot iterate over: string ("123-45-6789")`).
	merr := &migration.Error{
		TypeID:      "person",
		FromVersion: 2,
		ToVersion:   3,
		Err:         errors.New(`cannot iterate over: string ("123-45-6789")`),
	}

	// Replicated form: classifier only, no PII.
	require.NotContains(t, merr.RedactedMessage(), "123-45-6789")
	require.Contains(t, merr.RedactedMessage(), "person")
	require.Contains(t, merr.RedactedMessage(), "v2->v3")

	// Full form (node-local logs, errors chain) keeps the detail.
	require.Contains(t, merr.Error(), "123-45-6789")
}
