package cluster

import (
	"fmt"
	"strings"
)

// System-type compatibility namespace.
//
// A system type's UUID encodes how a node that does NOT know the type (an older
// binary during a rolling upgrade) must handle it — so the decision is
// structural, with no wire field and no registry lookup an old binary would
// fail. Layout of a namespaced system-type UUID:
//
//	[ 112-bit fixed recognizer prefix ][ 4-bit state ][ 12-bit index ]
//
// The 112-bit prefix (the leading 32 chars, dashes included) marks "committed
// system type"; it is forbidden to user type IDs. The 4-bit state is the compat
// class an unknowing node acts on; the 12-bit index makes each ID unique.
//
// The pre-namespace built-in types (random UUIDs, no prefix) are grandfathered:
// every current binary compiles them in, so they always resolve via the registry
// and never take the unknown path. Only types minted after the namespace shipped
// carry the prefix. See docs/api-compatibility.md.

const (
	// reservedSystemStrPrefix is the fixed 112-bit marker (32 chars including
	// dashes) every namespaced system-type UUID begins with; the trailing 4 hex
	// chars carry [1 hex: state][3 hex: index]. Distinctive enough that no user
	// UUID collides, and rejected from user type IDs at propose time regardless.
	reservedSystemStrPrefix = "c01177ed-0000-0000-0000-00000000"

	// Compat states (the 4-bit field). An unknowing node's rule on an unknown
	// reserved type: ungated → skip-and-warn; anything else (gated OR an
	// undefined state) → fatal. gated is the zero value, so the safe behavior is
	// the default.
	compatGated   uint8 = 0 // must-understand: fatal if unknown (emission still FeatureLevel-gated)
	compatUngated uint8 = 1 // skippable coordination/observability: skip if unknown
)

// reservedSystemClass reports whether id is a namespaced system-type UUID and,
// if so, its 4-bit compat state (the first of the trailing 4 hex chars). A
// non-UUID id, or one without the reserved prefix, is not ours (ok=false), so a
// user type ID — a name or a random UUID — always falls through unchanged.
func reservedSystemClass(id string) (state uint8, ok bool) {
	if len(id) != 36 || !strings.HasPrefix(id, reservedSystemStrPrefix) {
		return 0, false
	}
	switch c := id[32]; { // the state nibble
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// IsReservedSystemID reports whether id lies in committed's reserved system-type
// namespace (any state). User type IDs are rejected from it at propose time so a
// user can't author a type an older node would treat as a system record it
// should skip or gate.
func IsReservedSystemID(id string) bool {
	_, ok := reservedSystemClass(id)
	return ok
}

// reservedSystemID mints a namespaced system-type UUID for the given compat
// state and index — used to author built-in system types and by tests. Panics
// on an out-of-range state/index (a programming error at authoring time).
func reservedSystemID(state uint8, index uint16) string {
	if state > 0x0f {
		panic(fmt.Sprintf("reservedSystemID: state %d exceeds 4 bits", state))
	}
	if index > 0x0fff {
		panic(fmt.Sprintf("reservedSystemID: index %d exceeds 12 bits", index))
	}
	return fmt.Sprintf("%s%01x%03x", reservedSystemStrPrefix, state, index)
}

// UnknownReservedTypeError is returned by resolveType for a namespaced system
// type this binary does not know — a system type from a newer committed version.
// The apply path and the syncable reader inspect it: Skippable (an ungated type)
// → skip and advance; otherwise (gated, or an undefined state) → fatal, the loud
// backstop against a missed feature gate.
type UnknownReservedTypeError struct {
	ID    string
	State uint8
}

func (e *UnknownReservedTypeError) Error() string {
	return fmt.Sprintf("unknown reserved system type %q (compat state %d) — emitted by a newer committed version", e.ID, e.State)
}

// Skippable reports whether a node that doesn't know this type may skip it and
// advance past it — true only for the ungated state.
func (e *UnknownReservedTypeError) Skippable() bool {
	return e.State == compatUngated
}
