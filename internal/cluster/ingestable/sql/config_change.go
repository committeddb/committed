package sql

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
)

// primaryKeyChangeCode is the machine-readable code a deploy pipeline branches
// on to drive the delete + recreate recovery without scraping the message. It is
// surfaced generically via cluster.RebuildRequiredError.Code.
const primaryKeyChangeCode = "ingestable_primary_key_change_requires_recreate"

// PrimaryKeyChangeError reports that a re-POSTed ingestable config would change
// the primaryKey of an ingestable that already has a persisted config. The
// primaryKey defines the entity-key encoding (CompositeKey), the snapshot resume
// cursor (SnapshotProgress.LastPkByTable), and thus the downstream sink's row
// identity — but the persisted Position is inherited by ingestable id WITHOUT
// checking it was written under the same primaryKey. Applying the change in
// place would silently mis-page the resume cursor (duplicate rows) and orphan
// already-synced rows under their old keys. The fix is to delete + recreate the
// ingestable — which clears the Position (see NewDeleteIngestableEntities),
// forcing a clean full snapshot under the new keys — and rebuild the syncables
// that consume its topic.
//
// It implements cluster.RebuildRequiredError so the HTTP layer renders it (409 +
// code + structured details) without importing this package, and
// cluster.DependentsAware so the propose path (which owns the topology) can fill
// in the affected syncables.
type PrimaryKeyChangeError struct {
	// TopicID is the id of the topic this ingestable produces (its Type.ID) —
	// the topic whose entity keys the change re-keys.
	TopicID string `json:"topic"`
	// TopicName is the topic's human name, for the message and details.
	TopicName string `json:"topicName,omitempty"`
	// OldPrimaryKey / NewPrimaryKey are the persisted and incoming primary-key
	// column lists, so a pipeline can see exactly what changed.
	OldPrimaryKey []string `json:"oldPrimaryKey"`
	NewPrimaryKey []string `json:"newPrimaryKey"`
	// DependentSyncables are the syncables that consume TopicID and so must be
	// rebuilt after the recreate. Empty until the propose path fills it in via
	// SetDependents (this package doesn't know the syncable registry).
	DependentSyncables []cluster.DependentSyncable `json:"dependentSyncables,omitempty"`
}

func (e *PrimaryKeyChangeError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b,
		"ingestable primaryKey change (%s -> %s) will not be applied in place: the primaryKey defines the entity-key encoding and the persisted snapshot position is inherited under the old key, so an in-place change would duplicate rows and orphan already-synced rows under their old keys. Delete and recreate this ingestable (DELETE then POST /v1/ingestable/{id}) to re-snapshot under the new key, then rebuild the syncables that consume topic %s.",
		formatKey(e.OldPrimaryKey), formatKey(e.NewPrimaryKey), e.topicLabel())
	if len(e.DependentSyncables) > 0 {
		names := make([]string, 0, len(e.DependentSyncables))
		for _, d := range e.DependentSyncables {
			if d.Name != "" {
				names = append(names, fmt.Sprintf("%s (%s)", d.Name, d.ID))
			} else {
				names = append(names, d.ID)
			}
		}
		fmt.Fprintf(&b, " syncables to rebuild: %s.", strings.Join(names, ", "))
	}
	return b.String()
}

// topicLabel renders the topic for the message: "name (id)" when a name is
// known, else the bare id.
func (e *PrimaryKeyChangeError) topicLabel() string {
	if e.TopicName != "" {
		return fmt.Sprintf("%q (%s)", e.TopicName, e.TopicID)
	}
	return fmt.Sprintf("%q", e.TopicID)
}

// Code implements cluster.RebuildRequiredError.
func (e *PrimaryKeyChangeError) Code() string { return primaryKeyChangeCode }

// Details implements cluster.RebuildRequiredError: the exported fields (json
// tags above) are the machine-readable 409 payload.
func (e *PrimaryKeyChangeError) Details() any { return e }

// AffectedTopic implements cluster.DependentsAware: the topic whose consumers
// the propose path enumerates.
func (e *PrimaryKeyChangeError) AffectedTopic() string { return e.TopicID }

// SetDependents implements cluster.DependentsAware: the propose path hands back
// the syncables consuming AffectedTopic so they appear in the message + details.
func (e *PrimaryKeyChangeError) SetDependents(deps []cluster.DependentSyncable) {
	e.DependentSyncables = deps
}

// sourceChangeCode is the machine-readable code a deploy pipeline branches on to
// drive the delete + recreate recovery for a source-identity change.
const sourceChangeCode = "ingestable_source_change_requires_recreate"

// SourceIdentityChangeError reports that a re-POST changes an ingestable's
// SOURCE identity — the database server it reads (connectionString) or the topic
// it produces (Type.ID) — while a prior config exists. The persisted snapshot
// Position is inherited by ingestable id, so an in-place change would leave it
// stale: a server re-point resumes from a binlog/WAL position that does not
// exist on the new server (MySQL streams garbage or gaps; see mysql.go resume
// path), and a topic re-point starts the new topic mid-stream with no snapshot
// of the source's existing rows. The recovery is delete + recreate, which clears
// the Position (see NewDeleteIngestableEntities) and forces a clean full
// snapshot.
//
// Deliberately NOT flagged: a credential-only connectionString change (same host
// + database — a routine password rotation); a slot_name change (a recreated
// Postgres slot self-heals via the re-snapshot branch — the orphaned slot is a
// resource concern, not data loss); and a tables add/remove (the in-place
// reconcile owned by the publication / added-table backfill tickets).
//
// It implements cluster.RebuildRequiredError so the HTTP layer renders it (409 +
// code + details) without importing this package. It carries NO connection
// string (a secret) — only the names of the changed fields.
type SourceIdentityChangeError struct {
	TopicID       string   `json:"topic"`
	TopicName     string   `json:"topicName,omitempty"`
	ChangedFields []string `json:"changedFields"`
}

func (e *SourceIdentityChangeError) Error() string {
	return fmt.Sprintf(
		"ingestable source-identity change (%s) will not be applied in place: the persisted snapshot position is inherited by ingestable id. Changing the source (connectionString) leaves it pointing at a position that does not exist on the new server (streaming garbage or a gap); changing the produced topic starts the new topic mid-stream with no snapshot of the source's existing rows. Delete and recreate this ingestable (DELETE then POST /v1/ingestable/{id}) to re-snapshot cleanly. A credential-only connection-string change (same host and database) is not flagged.",
		strings.Join(e.ChangedFields, ", "))
}

// Code implements cluster.RebuildRequiredError.
func (e *SourceIdentityChangeError) Code() string { return sourceChangeCode }

// Details implements cluster.RebuildRequiredError.
func (e *SourceIdentityChangeError) Details() any { return e }

// serverIdentityChanged reports whether two connection strings name a different
// database SERVER — host, port, or database name — the part that makes a
// persisted binlog/WAL position meaningful. Credentials (user/password) are
// ignored so a routine password rotation is not flagged as a re-point. On a
// parse failure (a malformed string that nonetheless reached here) it falls back
// to a raw comparison: conservative, treating any change as a change.
func serverIdentityChanged(oldCS, newCS string) bool {
	if oldCS == newCS {
		return false
	}
	ou, oerr := ParseConnString(oldCS)
	nu, nerr := ParseConnString(newCS)
	if oerr != nil || nerr != nil {
		return true
	}
	return ou.Host != nu.Host || connDatabase(ou) != connDatabase(nu)
}

// connDatabase is the database name a connection-string URL addresses (the path
// with its leading slash trimmed), e.g. "postgres://h/app" -> "app".
func connDatabase(u *url.URL) string {
	return strings.TrimPrefix(u.Path, "/")
}

// formatKey renders a primary-key column list for a human message.
func formatKey(cols []string) string {
	if len(cols) == 0 {
		return "(none)"
	}
	return strings.Join(cols, ", ")
}

// primaryKeyEqual reports whether two primaryKey column lists produce the same
// entity-key encoding, so ValidateReplace only rejects a genuine re-key. The
// comparison is:
//   - length- and order-sensitive: CompositeKey marshals the values in column
//     order, so [a,b] and [b,a] re-key every row; and
//   - case-insensitive per column: CompositeKey looks up each value by the
//     LOWERCASED column name, so ["ID"] and ["id"] address the same value and
//     produce byte-identical keys — a pure case change re-keys nothing.
func primaryKeyEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}
