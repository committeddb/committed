package db_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// stubSchemaValidator is an injected cluster.TypeSchemaValidator whose verdict the
// test controls, so ProposeType's admission wiring is exercised without pulling in
// the http-layer schema compilers.
type stubSchemaValidator struct{ err error }

func (s stubSchemaValidator) ValidateTypeSchema(*cluster.Type) error { return s.err }

// TestProposeType_AdmissionSchemaCheck: ProposeType runs the injected schema
// validator and rejects a broken schema as a ConfigError (→ 400) rather than
// accepting the type to then fail every proposal to it (finding ① — a broken
// schema accepted at POST /type then a permanent 500 on every proposal).
func TestProposeType_AdmissionSchemaCheck(t *testing.T) {
	t.Run("rejects a broken schema as a ConfigError", func(t *testing.T) {
		d, _ := newWalDB(t)
		d.SetTypeSchemaValidator(stubSchemaValidator{err: errors.New("schema does not compile")})

		err := d.ProposeType(testCtx(t), createType("bad").config)
		require.Error(t, err)
		var cfgErr *cluster.ConfigError
		require.ErrorAs(t, err, &cfgErr, "a broken schema must be a ConfigError (400), not a raw error (500)")
		require.Contains(t, err.Error(), "schema does not compile")
	})

	t.Run("accepts when the validator passes", func(t *testing.T) {
		d, _ := newWalDB(t)
		d.SetTypeSchemaValidator(stubSchemaValidator{err: nil})
		require.NoError(t, d.ProposeType(testCtx(t), createType("ok").config))
	})

	t.Run("nil validator skips the check (back-compat)", func(t *testing.T) {
		d, _ := newWalDB(t)
		require.NoError(t, d.ProposeType(testCtx(t), createType("novalidator").config))
	})
}
