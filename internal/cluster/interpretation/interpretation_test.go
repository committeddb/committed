package interpretation

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func reg(t *testing.T, applied ...cluster.AppliedErratum) *Registry {
	t.Helper()
	r, err := NewRegistry(applied)
	require.NoError(t, err)
	return r
}

func eff(t *testing.T, r *Registry, typeID string, idx uint64, stamped int, payload string) int {
	t.Helper()
	v, err := r.EffectiveVersion(context.Background(), typeID, idx, stamped, []byte(payload))
	require.NoError(t, err)
	return v
}

// TestEffectiveVersion_RangeAndStampSelectors pins the fold's selectors: the
// inclusive index range, the fromVersion stamp narrowing (0 = any), rebinding
// down as well as up, and the errata-free / other-type fast paths.
func TestEffectiveVersion_RangeAndStampSelectors(t *testing.T) {
	r := reg(t,
		cluster.AppliedErratum{Index: 50, Erratum: cluster.Erratum{
			ID: "e1", TypeID: "photos", FromIndex: 10, ToIndex: 20, RebindToVersion: 2, FromVersion: 1,
		}},
		cluster.AppliedErratum{Index: 60, Erratum: cluster.Erratum{
			ID: "e2", TypeID: "photos", FromIndex: 30, ToIndex: 30, RebindToVersion: 1, FromVersion: 2,
		}},
	)

	// In range, stamped v1 → rebinds to 2; range bounds are inclusive.
	require.Equal(t, 2, eff(t, r, "photos", 10, 1, `{}`))
	require.Equal(t, 2, eff(t, r, "photos", 20, 1, `{}`))
	// Outside the range, or stamped another version: untouched.
	require.Equal(t, 1, eff(t, r, "photos", 9, 1, `{}`))
	require.Equal(t, 1, eff(t, r, "photos", 21, 1, `{}`))
	require.Equal(t, 3, eff(t, r, "photos", 15, 3, `{}`))
	// Rebinding DOWN: "wrongly stamped v2, read as v1".
	require.Equal(t, 1, eff(t, r, "photos", 30, 2, `{}`))
	// Another type entirely: the fast path.
	require.Equal(t, 1, eff(t, r, "orders", 15, 1, `{}`))
	require.Equal(t, uint64(60), r.Highwater())
	require.Equal(t, uint64(60), r.TypeHighwater("photos"))
	require.Equal(t, uint64(0), r.TypeHighwater("orders"))
}

// TestEffectiveVersion_LaterInLogWins pins the correction semantics: a wrong
// erratum is corrected by APPENDING another; among matching errata the higher
// raft index wins, regardless of build input order, and matching stays
// against the STAMPED version (never an intermediate rebound reading).
func TestEffectiveVersion_LaterInLogWins(t *testing.T) {
	wrong := cluster.AppliedErratum{Index: 50, Erratum: cluster.Erratum{
		ID: "wrong", TypeID: "photos", FromIndex: 10, ToIndex: 20, RebindToVersion: 2, FromVersion: 1,
	}}
	fix := cluster.AppliedErratum{Index: 70, Erratum: cluster.Erratum{
		ID: "fix", TypeID: "photos", FromIndex: 10, ToIndex: 20, RebindToVersion: 3, FromVersion: 1,
	}}

	// Same answer whichever order the applied records arrive in.
	require.Equal(t, 3, eff(t, reg(t, wrong, fix), "photos", 15, 1, `{}`))
	require.Equal(t, 3, eff(t, reg(t, fix, wrong), "photos", 15, 1, `{}`))

	// The fix matches on the STAMP (v1): an entity stamped v2 in the range is
	// untouched by both — no chained interpretation.
	require.Equal(t, 2, eff(t, reg(t, wrong, fix), "photos", 15, 2, `{}`))
}

// TestEffectiveVersion_PredicateBinding pins the interleaved-writers case:
// within one range, only payloads the predicate maps to LITERAL true rebind —
// jq truthiness is deliberately not enough.
func TestEffectiveVersion_PredicateBinding(t *testing.T) {
	r := reg(t, cluster.AppliedErratum{Index: 50, Erratum: cluster.Erratum{
		ID: "e", TypeID: "photos", FromIndex: 10, ToIndex: 20, RebindToVersion: 2,
		Predicate: `.writer == "app-b"`,
	}})

	require.Equal(t, 2, eff(t, r, "photos", 15, 1, `{"writer":"app-b"}`))
	require.Equal(t, 1, eff(t, r, "photos", 15, 1, `{"writer":"app-a"}`))
	// A non-boolean truthy result is NOT a match (accidental match-alls).
	r2 := reg(t, cluster.AppliedErratum{Index: 50, Erratum: cluster.Erratum{
		ID: "e", TypeID: "photos", FromIndex: 10, ToIndex: 20, RebindToVersion: 2,
		Predicate: `.writer`,
	}})
	require.Equal(t, 1, eff(t, r2, "photos", 15, 1, `{"writer":"app-b"}`))

	// A payload the predicate cannot evaluate is an ERROR, never a silent
	// fall-through to a possibly-wrong reading.
	_, err := r.EffectiveVersion(context.Background(), "photos", 15, 1, []byte("not json"))
	require.Error(t, err)
}

// fakeInner records the actuals it receives.
type fakeInner struct{ got []*cluster.Actual }

func (f *fakeInner) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	f.got = append(f.got, a)
	return true, nil
}
func (f *fakeInner) Close() error { return nil }

type stubResolver struct{ types map[string]*cluster.Type }

func (r *stubResolver) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	if t, ok := r.types[fmt.Sprintf("%s@%d", ref.ID, ref.Version)]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("type %s@%d not found", ref.ID, ref.Version)
}

// TestWrap_RebindsOnlyMatchingRows pins the wrapper: matching rows arrive
// carrying the effective version's Type, everything else — non-matching rows,
// deletes, and the untouched-actual fast path — passes through IDENTICALLY
// (same slice, no copy).
func TestWrap_RebindsOnlyMatchingRows(t *testing.T) {
	v1 := &cluster.Type{ID: "photos", Version: 1}
	v2 := &cluster.Type{ID: "photos", Version: 2}
	res := &stubResolver{types: map[string]*cluster.Type{"photos@2": v2}}
	r := reg(t, cluster.AppliedErratum{Index: 50, Erratum: cluster.Erratum{
		ID: "e", TypeID: "photos", FromIndex: 10, ToIndex: 20, RebindToVersion: 2, FromVersion: 1,
	}})

	inner := &fakeInner{}
	wrapped := Wrap(inner, func() *Registry { return r }, res)

	inRange := &cluster.Actual{Index: 15, Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(v1, []byte("k1"), []byte(`{}`)),
		cluster.NewDeleteEntity(v1, []byte("k2")),
	}}
	outOfRange := &cluster.Actual{Index: 25, Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(v1, []byte("k3"), []byte(`{}`)),
	}}

	_, err := wrapped.Sync(context.Background(), inRange)
	require.NoError(t, err)
	_, err = wrapped.Sync(context.Background(), outOfRange)
	require.NoError(t, err)

	require.Equal(t, 2, inner.got[0].Entities[0].Version, "the in-range row is rebound to v2")
	require.Equal(t, 1, inner.got[0].Entities[1].Version, "the delete passes through unrebound (no payload to reinterpret)")
	require.Same(t, outOfRange, inner.got[1], "an untouched actual passes through without copying")
}

// BenchmarkEffectiveVersion_ErrataFree pins the zero-cost claim: a topic with
// no errata resolves in one nil-map lookup — no allocation, low single-digit
// nanoseconds — so the read path is free until the first erratum exists.
func BenchmarkEffectiveVersion_ErrataFree(b *testing.B) {
	r, err := NewRegistry(nil)
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(`{"caption":"a"}`)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := r.EffectiveVersion(ctx, "photos", uint64(i), 1, payload)
		if err != nil || v != 1 {
			b.Fatal(v, err)
		}
	}
}

// BenchmarkEffectiveVersion_OtherTypeUntouched: errata exist, but for another
// type — the per-type map keeps unaffected topics on the fast path.
func BenchmarkEffectiveVersion_OtherTypeUntouched(b *testing.B) {
	r, err := NewRegistry([]cluster.AppliedErratum{{Index: 50, Erratum: cluster.Erratum{
		ID: "e", TypeID: "orders", FromIndex: 1, ToIndex: 1000, RebindToVersion: 2,
	}}})
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(`{"caption":"a"}`)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := r.EffectiveVersion(ctx, "photos", uint64(i), 1, payload)
		if v != 1 {
			b.Fatal(v)
		}
	}
}
