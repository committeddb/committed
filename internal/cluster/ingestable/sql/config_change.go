package sql

import (
	"fmt"
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
