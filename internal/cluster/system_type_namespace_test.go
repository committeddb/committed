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
	// A grandfathered built-in system UUID (random, no reserved prefix).
	_, ok := reservedSystemClass(databaseType.ID)
	require.False(t, ok)
	// A user-defined name is not a UUID and not reserved; nor is the empty id.
	_, ok = reservedSystemClass("controlplane-event")
	require.False(t, ok)
	_, ok = reservedSystemClass("")
	require.False(t, ok)
}

// ingestableStuck is the first re-keyed built-in: it must be a namespaced,
// UNGATED type (so an older node skips it) that still resolves as internal via
// the registry on a binary that knows it.
func TestIngestableStuck_IsUngatedNamespaced(t *testing.T) {
	state, ok := reservedSystemClass(ingestableStuckType.ID)
	require.True(t, ok, "ingestableStuck must be in the reserved system-type namespace")
	require.Equal(t, compatUngated, state, "a terminal-park record is skippable observability")
	require.True(t, IsInternal(ingestableStuckType.ID), "still resolved as internal via the registry")
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

// legacyGrandfatheredSystemTypeIDs pins the pre-namespace built-in system types
// by their exact (random) UUIDs. They predate the compat namespace; every
// current binary compiles them all in, so they always resolve via the registry
// and never take an unknown-type path. New system types must NOT be added here —
// they carry a reservedSystemID (see TestSystemTypesAreClassifiedForUpgrade).
var legacyGrandfatheredSystemTypeIDs = map[string]bool{
	"0cd18065-a0e2-4c19-a4d6-f824f1898cb5": true, // syncable
	"ab972bba-83fe-4dea-9c5d-877645e8d21e": true, // syncableIndex
	"5f3b6c8e-1d2a-4e7b-9c0f-2a8d6b4e1f93": true, // syncableDeadLetter
	"8a1c4d2e-7b3f-4a6c-9e8d-1f5b2c7a9d04": true, // syncableStuck
	"3d9e6b1a-5c2f-4d7b-8a0e-6f4c1b8d3e25": true, // syncableSkipRequest
	"268e1ac4-7d17-4798-afae-3f1f9aa6fc65": true, // type
	"65499eaa-5910-4798-8cc5-0c2d996658e3": true, // nodeAPIURL
	"45a0b2d1-99e7-4cf2-958c-a7c7e797d3ab": true, // scrub
	"c5917145-c248-4d97-a863-8e26ca042b09": true, // ingestable
	"8ea60a68-e22a-41cd-b09d-31352b0356f1": true, // ingestablePosition
	"4698b77e-9a7c-41a2-aae4-984da0cd33c1": true, // database
	"a4223c88-30c9-4b50-9948-4b6ed096e84b": true, // nodeVersion
	"9e9a9e5f-22f6-4963-ae77-a4a87d807496": true, // typeMigrationDeadLetter
}

// systemTypeClassified reports whether id is an acceptable built-in system-type
// id: a grandfathered legacy id, or a namespaced id with a DEFINED compat state.
func systemTypeClassified(id string) bool {
	if legacyGrandfatheredSystemTypeIDs[id] {
		return true
	}
	state, ok := reservedSystemClass(id)
	return ok && (state == compatGated || state == compatUngated)
}

// TestSystemTypesAreClassifiedForUpgrade is the tripwire the ingestableStuck
// lapse was missing: every registered system type must be either grandfathered
// (a pinned pre-namespace UUID) or in the reserved namespace with a defined
// compat state. Adding a registerSystemType without doing one of those fails
// here — mint the new id with reservedSystemID(compatGated|compatUngated, index),
// do NOT extend the legacy set.
func TestSystemTypesAreClassifiedForUpgrade(t *testing.T) {
	for id := range systemTypes {
		require.Truef(t, systemTypeClassified(id),
			"system type %q is neither grandfathered nor a namespaced type with a defined compat state; "+
				"mint its id with reservedSystemID(compatGated or compatUngated, index) — a new type must not be added to the legacy set", id)
	}

	// No stale pins: a removed/renamed grandfathered built-in must be a
	// deliberate change that also updates this set.
	for id := range legacyGrandfatheredSystemTypeIDs {
		require.Containsf(t, systemTypes, id,
			"grandfathered id %q is no longer registered — update legacyGrandfatheredSystemTypeIDs deliberately", id)
	}
}

// TestSystemTypeClassified_RejectsUnclassified red-proves the tripwire's
// enforcement: an unclassified id (a random UUID, or a namespaced one with an
// undefined state) is rejected, so a future ungrouped registerSystemType fails
// the tripwire above.
func TestSystemTypeClassified_RejectsUnclassified(t *testing.T) {
	require.True(t, systemTypeClassified(databaseType.ID))                  // grandfathered
	require.True(t, systemTypeClassified(ingestableStuckType.ID))           // namespaced ungated
	require.True(t, systemTypeClassified(reservedSystemID(compatGated, 5))) // namespaced gated

	require.False(t, systemTypeClassified("f7e6d5c4-1111-2222-3333-444455556666")) // random, unclassified
	require.False(t, systemTypeClassified(reservedSystemID(0x0f, 1)))              // namespaced, undefined state
	require.False(t, systemTypeClassified("controlplane-event"))                   // not even a UUID
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
