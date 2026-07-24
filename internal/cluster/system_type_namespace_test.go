package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedSystemNamespace_RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		state uint8
		index uint16
	}{
		{compatGated, 0},
		{compatUngated, 0},
		{compatUngated, 4095}, // max 12-bit index
		{0x0f, 4095},          // max 4-bit state (an undefined class, but a valid ID)
	} {
		id := reservedSystemID(tc.state, tc.index)
		state, ok := reservedSystemClass(id)
		require.Truef(t, ok, "%s should be recognized as reserved", id)
		require.Equalf(t, tc.state, state, "state round-trip for %s", id)
	}

	require.NotEqual(t, reservedSystemID(compatUngated, 1), reservedSystemID(compatUngated, 2))
	require.NotEqual(t, reservedSystemID(compatGated, 1), reservedSystemID(compatUngated, 1))
}

func TestReservedSystemNamespace_NonReserved(t *testing.T) {
	// A legacy (grandfathered) system UUID has no reserved prefix.
	_, ok := reservedSystemClass("d3f5a7b9-2e4c-4f6a-8b1d-3c5e7a9f0b24")
	require.False(t, ok)
	// A user-defined name is not a UUID and not reserved; nor is the empty id.
	_, ok = reservedSystemClass("controlplane-event")
	require.False(t, ok)
	_, ok = reservedSystemClass("")
	require.False(t, ok)
}

func TestReservedSystemID_PanicsOutOfRange(t *testing.T) {
	require.Panics(t, func() { reservedSystemID(0x10, 0) })   // state > 4 bits
	require.Panics(t, func() { reservedSystemID(0, 0x1000) }) // index > 12 bits
}

func TestUnknownReservedTypeError_Skippable(t *testing.T) {
	require.True(t, (&UnknownReservedTypeError{State: compatUngated}).Skippable())
	require.False(t, (&UnknownReservedTypeError{State: compatGated}).Skippable())
	require.False(t, (&UnknownReservedTypeError{State: 7}).Skippable()) // undefined class → not skippable
}

func TestResolveType_ReservedUnknownVsUserUnknown(t *testing.T) {
	empty := &stubResolver{} // errors on any lookup

	// A registered (built-in) type resolves via the registry, never the namespace.
	got, err := resolveType(TypeRef{ID: databaseType.ID}, empty)
	require.NoError(t, err)
	require.Equal(t, databaseType.ID, got.ID)

	// A namespaced ungated type this binary doesn't register → typed, skippable error.
	id := reservedSystemID(compatUngated, 4000)
	_, err = resolveType(TypeRef{ID: id}, empty)
	var ure *UnknownReservedTypeError
	require.ErrorAs(t, err, &ure)
	require.True(t, ure.Skippable())
	require.Equal(t, id, ure.ID)

	// A namespaced gated unknown → typed error, NOT skippable.
	_, err = resolveType(TypeRef{ID: reservedSystemID(compatGated, 4000)}, empty)
	require.ErrorAs(t, err, &ure)
	require.False(t, ure.Skippable())

	// A user-defined unknown → falls through to the resolver, NOT our typed error.
	_, err = resolveType(TypeRef{ID: "some-user-type"}, empty)
	require.Error(t, err)
	var notReserved *UnknownReservedTypeError
	require.False(t, errors.As(err, &notReserved))
}
