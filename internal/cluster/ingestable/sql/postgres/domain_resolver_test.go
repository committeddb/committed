package postgres

import (
	"context"
	gosql "database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestOIDCategoryResolver_TransientErrorNotCached is the domain-resolver
// miscache regression: a transient pg_type lookup failure on the FIRST
// classification of a domain OID must not permanently cache CatText for that OID.
// Pre-fix, category cached resolveDomain's result unconditionally — including the
// error fallback — so a single connection blip rendered every later value of a
// domain-over-numeric column as a JSON string for the rest of the session,
// diverging from the snapshot (which classifies by base type) and defeating dedup.
func TestOIDCategoryResolver_TransientErrorNotCached(t *testing.T) {
	const domainOID = uint32(999999) // not a base type — classified via pg_type

	calls := 0
	failNext := true
	r := &oidCategoryResolver{
		cache: map[uint32]sql.JSONCategory{},
		lookupType: func(_ context.Context, oid uint32) (string, uint32, error) {
			calls++
			if failNext {
				failNext = false
				return "", 0, errors.New("connection reset by peer") // transient blip
			}
			switch oid {
			case domainOID:
				return "d", pgtype.NumericOID, nil // a DOMAIN over NUMERIC
			case pgtype.NumericOID:
				return "b", 0, nil // NUMERIC is a base type
			default:
				return "", 0, gosql.ErrNoRows
			}
		},
	}
	ctx := context.Background()

	// First classification hits the transient error: CatText for THIS value, but
	// it must NOT be cached.
	require.Equal(t, sql.CatText, r.category(ctx, domainOID))
	require.Empty(t, r.cache, "a transient-error resolution must not be cached")

	// The next value retries: the lookup now succeeds and the domain resolves to
	// its NUMERIC base — a number, matching the snapshot. The pre-fix bug returned
	// the stuck CatText here instead.
	require.Equal(t, sql.CatNumber, r.category(ctx, domainOID))
	require.Equal(t, sql.CatNumber, r.cache[domainOID], "a real resolution is cached")

	// A third call is served from cache — no further lookups (no per-value storm).
	callsBefore := calls
	require.Equal(t, sql.CatNumber, r.category(ctx, domainOID))
	require.Equal(t, callsBefore, calls, "a cached category must not re-query")
}
