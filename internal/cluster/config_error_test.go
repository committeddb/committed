package cluster_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func TestUnknownDialectError(t *testing.T) {
	e := cluster.UnknownDialectError("nope", []string{"postgres", "mysql"})
	require.Equal(t, "sql.dialect", e.Field)
	// Valid list is sorted for a stable message.
	require.Equal(t, `sql.dialect: unknown dialect "nope"; valid: mysql, postgres`, e.Error())

	missing := cluster.UnknownDialectError("", []string{"postgres", "mysql"})
	require.Equal(t, "sql.dialect: required; valid dialects: mysql, postgres", missing.Error())
}

func TestNewConfigError_LiftsFieldContext(t *testing.T) {
	fe := &cluster.FieldError{Field: "sql.db", Issue: `database "x" not found`}
	ce := cluster.NewConfigError(fe)
	require.Equal(t, "sql.db", ce.Field)
	require.Equal(t, `database "x" not found`, ce.Issue)
	require.Equal(t, `sql.db: database "x" not found`, ce.Error())

	// A non-FieldError cause yields a ConfigError with no field context.
	plain := cluster.NewConfigError(errors.New("boom"))
	require.Empty(t, plain.Field)
	require.Empty(t, plain.Issue)
	require.Equal(t, "boom", plain.Error())
}

func TestFieldError_UnwrapsCause(t *testing.T) {
	cause := errors.New("missing")
	fe := &cluster.FieldError{Field: "sql.db", Issue: "database not found", Err: cause}
	require.ErrorIs(t, fe, cause)
	// And through a ConfigError wrap (the Propose* path).
	require.ErrorIs(t, cluster.NewConfigError(fe), cause)
}
