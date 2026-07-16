package sql

import (
	"fmt"
	"sort"
	"strings"
)

// SyncableIdentity is the set of config fields that make a syncable's persisted
// SyncableIndex checkpoint meaningful: the topic(s) it consumes, the destination
// database, and the destination table it materializes. The checkpoint records
// "consumed the topic up to raft index N and wrote it into <database>.<table>",
// and is inherited by syncable id on re-POST. It is valid ONLY while these
// fields are unchanged — re-point the topic, the database, or rename the table
// and the inherited checkpoint makes the new destination materialize only from
// index N forward: the pre-N history is never written (silent data loss) and the
// old table is orphaned. Such a change is rejected so the operator resets the
// checkpoint via delete + recreate.
//
// This is the identity sibling of SyncableSchema: SyncableSchema guards a
// silent no-op on the SAME table (CREATE TABLE IF NOT EXISTS never ALTERs);
// SyncableIdentity guards a stale checkpoint when the table, topic, or database
// itself changes. The two are checked together in validateReplace, identity
// first.
type SyncableIdentity struct {
	// Topics are the topics the syncable consumes, sorted for order-insensitive
	// comparison: one for a plain syncable, N for a multi-source projection.
	Topics []string
	// Database is the id of the destination [database] config (sql.db). A
	// re-point at a different database with the checkpoint inherited by id
	// materializes the new database only from the checkpoint forward.
	Database string
	// Table is the destination table the syncable materializes.
	Table string
}

// identityOf reads the identity of a plain (single-topic) syncable config.
func identityOf(c *Config) SyncableIdentity {
	return SyncableIdentity{Topics: []string{c.Topic}, Database: c.DatabaseID, Table: c.Table}
}

// identityChangeCode is the machine-readable code a deploy pipeline branches on.
const identityChangeCode = "config_identity_change_requires_rebuild"

// IdentityChangeError reports that a re-POST changes a syncable's identity — the
// topic it consumes, the destination database, or the table it materializes —
// while a prior config exists. Because the SyncableIndex checkpoint is inherited
// by id, applying the change in place would materialize the new destination only
// from the checkpoint forward (old table orphaned, pre-checkpoint history
// missing): silent data loss. It implements cluster.RebuildRequiredError so the
// HTTP layer renders it as 409 + code + details without importing this package.
type IdentityChangeError struct {
	ChangedFields []string `json:"changedFields"`
	OldTopics     []string `json:"oldTopics,omitempty"`
	NewTopics     []string `json:"newTopics,omitempty"`
	OldDatabase   string   `json:"oldDatabase,omitempty"`
	NewDatabase   string   `json:"newDatabase,omitempty"`
	OldTable      string   `json:"oldTable,omitempty"`
	NewTable      string   `json:"newTable,omitempty"`
}

func (e *IdentityChangeError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "syncable identity change (%s) will not be applied in place: its SyncableIndex checkpoint is inherited by id, so the new destination would be materialized only from the checkpoint forward — the pre-checkpoint history is never written and the old table is orphaned. Reset the checkpoint by deleting and recreating the syncable (DELETE then POST /v1/syncable/{id}); a same-id recreate starts fresh from index 0.", strings.Join(e.ChangedFields, ", "))
	if e.OldDatabase != e.NewDatabase {
		fmt.Fprintf(&b, " database: %q -> %q.", e.OldDatabase, e.NewDatabase)
	}
	if e.OldTable != e.NewTable {
		fmt.Fprintf(&b, " table: %q -> %q.", e.OldTable, e.NewTable)
	}
	if !equalStrings(e.OldTopics, e.NewTopics) {
		fmt.Fprintf(&b, " topics: [%s] -> [%s].", strings.Join(e.OldTopics, ", "), strings.Join(e.NewTopics, ", "))
	}
	return b.String()
}

// Code implements cluster.RebuildRequiredError.
func (e *IdentityChangeError) Code() string { return identityChangeCode }

// Details implements cluster.RebuildRequiredError.
func (e *IdentityChangeError) Details() any { return e }

// identityChange returns an *IdentityChangeError if the syncable's identity
// (consumed topics or destination table) changed, or nil if it is unchanged.
// Topic comparison is order-insensitive (a projection's sources may be
// reordered without changing what it consumes).
func identityChange(old, next SyncableIdentity) *IdentityChangeError {
	oldTopics := sortedCopy(old.Topics)
	nextTopics := sortedCopy(next.Topics)

	var changed []string
	if !equalStrings(oldTopics, nextTopics) {
		changed = append(changed, "topic")
	}
	if old.Database != next.Database {
		changed = append(changed, "database")
	}
	if old.Table != next.Table {
		changed = append(changed, "table")
	}
	if len(changed) == 0 {
		return nil
	}
	return &IdentityChangeError{
		ChangedFields: changed,
		OldTopics:     oldTopics,
		NewTopics:     nextTopics,
		OldDatabase:   old.Database,
		NewDatabase:   next.Database,
		OldTable:      old.Table,
		NewTable:      next.Table,
	}
}

// sortedCopy returns a sorted copy of s, leaving the input untouched.
func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

// equalStrings reports whether two string slices are element-wise equal.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
