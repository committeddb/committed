package wal

import "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"

// eventLogBinding identifies one published storage lifetime. Logical operations
// use entries. Native maintenance capabilities are separate: their layouts and
// physical sequences are not part of the entryStore contract. Replacement swaps
// the entire binding under eventMu, invalidating reader source identities.
type eventLogBinding struct {
	entries entryStore
	native  *tidwall.LegacyLog
}

func (b *eventLogBinding) Close() error { return b.entries.Close() }
