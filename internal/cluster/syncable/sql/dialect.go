package sql

import (
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/sqlident"
)

type Dialect interface {
	CreateDDL(config *Config) string
	// DropDDL returns the destructive mirror of CreateDDL:
	// `DROP TABLE IF EXISTS <table>`. It backs the syncable Teardown used by
	// delete/rebuild. IF EXISTS keeps it idempotent; DROP TABLE removes the
	// table's indexes with it.
	DropDDL(config *Config) string
	CreateSQL(config *Config) string
	// CreateGenerationUpsertSQL is CreateSQL with the committed-managed
	// GenerationColumn appended as the final column/placeholder (and, for a keyed
	// upsert, its update assignment). Only the keyed plain Syncable uses it, so a
	// refresh-boundary sweep can find rows carrying an older epoch; projections
	// and keyless syncables keep the plain CreateSQL. applyEntity appends the
	// entity's epoch after the mapped values to fill the extra placeholder.
	CreateGenerationUpsertSQL(config *Config) string
	// EnsureGenerationColumn idempotently adds the committed-managed
	// GenerationColumn to an EXISTING keyed sink (one created before the
	// reconciling-refresh feature, or freshly created by CreateDDL — which does
	// not carry the column). It is a no-op when the column already exists, so
	// Init can call it unconditionally on every keyed syncable. Existing rows are
	// baselined to generation 1 (the initial-snapshot epoch) so the first
	// gap-recovery refresh — at a later epoch — still reconciles a pre-existing
	// row that has since vanished at the source. Postgres uses ADD COLUMN IF NOT
	// EXISTS; MySQL (no such clause) checks information_schema first.
	EnsureGenerationColumn(db *gosql.DB, config *Config) error
	// CreateGenerationSweepSQL returns the reconciling sweep a keyed sink runs on
	// a refresh-boundary marker: `DELETE FROM <table> WHERE <GenerationColumn>
	// >= 1 AND <GenerationColumn> < <placeholder>`. Its single bound argument is
	// the marker's epoch G. The `>= 1` floor protects generation-0 rows (direct
	// user writes, pre-feature rows never re-upserted) from a sweep; `< G` removes
	// any row still carrying an older epoch — the rows a positive re-enumeration
	// could not signal because they were deleted at the source in the lost window.
	CreateGenerationSweepSQL(config *Config) string
	// CreateDeleteSQL returns the statement that removes one downstream row
	// by its key column: `DELETE FROM <table> WHERE <keyCol> = <placeholder>`.
	// The placeholder is dialect-specific (? for MySQL, $1 for PostgreSQL).
	// The key column is Config.DeleteKeyColumn(); the single bound argument
	// is the entity's Key, so deletes never unmarshal the (absent) payload.
	CreateDeleteSQL(config *Config) string
	// CreateClearSQL returns the statement that NULLs a set of columns for one
	// row by its key column: `UPDATE <table> SET <c1>=NULL,<c2>=NULL WHERE
	// <keyCol> = <placeholder>`. It backs a multi-source projection's
	// `onDelete = "clear"` — a contributor source's delete blanks the columns it
	// owns without dropping the folded row. An UPDATE (not an upsert) so a clear
	// against an absent row is a no-op, never a ghost row. Placeholder is
	// dialect-specific; the single bound argument is the entity's Key.
	CreateClearSQL(config *Config, columns []string) string
	// CreateAggregateSidecarDDL returns the CREATE TABLE for an aggregate
	// source's backing table — the normalized store whose rows materialize the
	// parent's JSON-array column. Its columns are fixed (Sidecar* consts):
	// child_key (PK, the aggregated entity's Key), parent_key (the correlation
	// key), element_key (the sort/identity key, stored as text), and element
	// (the per-child JSON object). The dialect picks its own JSON and key column
	// types (JSONB vs JSON, TEXT vs VARCHAR). Dropped via DropDDL on the sidecar
	// table name, so no separate drop method is needed.
	CreateAggregateSidecarDDL(spec AggregateSpec) string
	// CreateAggregateMaterializeSQL returns the upsert that (re)writes the
	// parent row's array column from the sidecar: INSERT the parent row with
	// column = agg(elements for this parent ORDER BY element_key) ON CONFLICT DO
	// UPDATE. An upsert (not a plain UPDATE) so a child arriving before its
	// spine still lands its collection on a fresh partial row, exactly as a
	// scalar contributor's rule upsert does. Both placeholders bind the parent
	// key (the dialect repeats it rather than relying on placeholder reuse, so
	// the runtime binds the same two-arg shape for every dialect).
	CreateAggregateMaterializeSQL(spec AggregateSpec) string
	// CreateAggregateRebuildSQL returns the delete-path UPDATE that rewrites the
	// parent's array column from the sidecar after a child was removed: an
	// UPDATE (never an upsert) so removing the last child of an absent parent is
	// a no-op, never a ghost row. Both placeholders bind the parent key.
	CreateAggregateRebuildSQL(spec AggregateSpec) string
	// CreateAggregateParentLookupSQL returns the SELECT that recovers a removed
	// child's parent key from the sidecar by its child key. A delete Actual
	// carries no payload, so the parent correlation is read back from the
	// sidecar the upsert recorded; the single bound argument is the child Key.
	CreateAggregateParentLookupSQL(spec AggregateSpec) string
	// CreateLookupDimensionDDL returns the CREATE TABLE for an enrichment
	// dimension: lookup_key (PK, the foreign-key target) and lookup_fields (the
	// stored JSON object of dimension columns). The dialect picks its own JSON
	// and key types. Dropped via DropDDL on the dimension table name.
	CreateLookupDimensionDDL(spec LookupSpec) string
	// CreateAggregateAffectedParentsSQL returns the fan-out query: the DISTINCT
	// parent keys whose folded children reference a given dimension key —
	// `SELECT DISTINCT parent_key FROM <sidecar> WHERE element->>'<onField>' =
	// <placeholder>`. When a dimension row changes, these are the parents whose
	// array column must be re-materialized. The single bound argument is the
	// changed dimension key.
	CreateAggregateAffectedParentsSQL(spec AggregateSpec, onField string) string
	// CreateAppliedSidecarDDL returns the CREATE TABLE for a keyless (append)
	// syncable's dedup sidecar: (committed_index, committed_seq) under a composite
	// PRIMARY KEY. committed marks each applied row here so replaying the same
	// Actual is a no-op — giving an append/history table exactly-once semantics
	// without adding committed-managed columns to the user's own table. The
	// sidecar name is AppliedSidecarName(config.Table); dropped via DropDDL on it.
	CreateAppliedSidecarDDL(config *Config) string
	// CreateAppliedMarkSQL returns the mark-and-detect insert into the applied
	// sidecar: `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL) / `INSERT IGNORE`
	// (MySQL). RowsAffected is 1 on a first apply and 0 on a replay, which the
	// runtime uses to skip the (non-idempotent) history insert on replay. Its two
	// placeholders bind the Actual's raft index and the entity's ordinal.
	CreateAppliedMarkSQL(config *Config) string
	Open(connectionString string) (*gosql.DB, error)
	// IsPermanent returns true if the given SQL error is non-retryable
	// (e.g., constraint violations, data-type mismatches). The sync loop
	// skips proposals that produce permanent errors instead of retrying.
	IsPermanent(err error) bool
	// BindArgs arranges one row's mapped column values into the positional
	// arguments that this dialect's CreateSQL placeholders expect. MySQL's
	// `INSERT ... ON DUPLICATE KEY UPDATE col = ?` repeats every column, so
	// it needs the values twice; PostgreSQL's `ON CONFLICT ... DO UPDATE SET
	// col = EXCLUDED.col` references the proposed row and needs them once.
	// The Syncable is otherwise dialect-agnostic, so it delegates this
	// arrangement here rather than hardcoding the MySQL doubling.
	BindArgs(values []any) []any
}

// The mapstructure tags drive viper.UnmarshalKey when parsing the
// [[sql.indexes]] / [[sql.mappings]] array-of-tables. They're required
// where the Go field name differs from the TOML key (IndexName→name,
// ColumnNames→index, SQLType→type) and make the camelCase keys
// (jsonPath) explicit so parsing no longer depends on viper's key-case
// handling, which changed between viper versions.
type Index struct {
	IndexName   string `mapstructure:"name"`
	ColumnNames string `mapstructure:"index"` // comma separated list of columns - why isn't this a slice?
}

type Mapping struct {
	JsonPath string `mapstructure:"jsonPath"`
	Column   string `mapstructure:"column"`
	SQLType  string `mapstructure:"type"`
	// TODO Add a concept of an optional mapping that doesn't error if it is missing
}

// wholePayloadPath is the jsonPath that maps the entire submitted document —
// the raw payload bytes — into a single column, rather than extracting one
// leaf value. It is the conventional event-log shape: a few scalar envelope
// columns for indexing plus one payload column the read side folds.
const wholePayloadPath = "$"

// GenerationColumn is the committed-managed column a KEYED sink carries to track
// the ingest refresh epoch that last wrote each row (see cluster.Entity.
// Generation). committed adds it to the sink itself (via EnsureGenerationColumn,
// not the user's mappings) and stamps every upsert with the entity's epoch; a
// refresh-boundary marker then sweeps rows whose generation is older than the
// refresh (CreateGenerationSweepSQL). It is namespaced to avoid colliding with a
// user column; validateMappings rejects a config that maps to this name.
const GenerationColumn = "committed_generation"

// wholePayloadColumnTypes are the case-insensitive column-type prefixes a
// whole-payload mapping may target. The raw document binds as a JSON string,
// so the column must hold arbitrary text or native JSON ("JSON" also covers
// JSONB). Anything else (INT, BOOLEAN, TIMESTAMP, …) would fail only at exec
// time, with a driver bind error that Dialect.IsPermanent cannot classify —
// leaving the sync worker retrying forever — so it is rejected at config
// time instead.
var wholePayloadColumnTypes = []string{
	"JSON", "TEXT", "VARCHAR", "NVARCHAR", "CHAR", "LONGTEXT", "MEDIUMTEXT", "CLOB",
}

// validateMappings rejects mapping configurations that could otherwise fail
// only at exec time. Sole rule today: a whole-payload ("$") mapping must
// target a column type that can hold the raw JSON document.
func validateMappings(mappings []Mapping) error {
	for _, m := range mappings {
		// GenerationColumn is committed-managed: a keyed sink's CreateSQL appends
		// it and EnsureGenerationColumn creates it. A user mapping to the same
		// name would collide (a duplicate column in CREATE/INSERT), failing only
		// at Init — so reject it at config time with an actionable message.
		if strings.EqualFold(strings.TrimSpace(m.Column), GenerationColumn) {
			return fmt.Errorf(
				"mapping column %q is reserved: committed manages a %q column on keyed sinks for reconciling refreshes; rename this mapping",
				m.Column, GenerationColumn)
		}
		if m.JsonPath != wholePayloadPath {
			continue
		}
		sqlType := strings.ToUpper(strings.TrimSpace(m.SQLType))
		allowed := false
		for _, prefix := range wholePayloadColumnTypes {
			if strings.HasPrefix(sqlType, prefix) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf(
				"whole-payload mapping for column %q: jsonPath %q binds the entire payload as JSON text and requires a JSON or text column type (JSONB, JSON, TEXT, VARCHAR, …); got %q",
				m.Column, wholePayloadPath, m.SQLType)
		}
	}
	return nil
}

// validateConfigIdentifiers gates the config-supplied SQL identifiers and type
// expressions at config time, so a bad one is a clean 400 rather than a deferred
// driver error (or, before the identifier-quoting seam, a raw interpolation).
// Every identifier is quoted before it reaches SQL — so this is a conservative
// guard (reject control chars / empty), NOT the injection defense; a type
// expression cannot be quoted, so SQLType gets a strict charset. table may be
// schema-qualified (a dot is a legitimate separator, so it is validated as a
// whole rather than rejected for the dot). Empty table / mappings are already
// rejected upstream, and an empty primaryKey/keyColumn means "not set", so those
// are validated only when present.
func validateConfigIdentifiers(table, primaryKey, keyColumn string, mappings []Mapping, indexes []Index) error {
	if !sqlident.ValidIdent(table) {
		return &cluster.FieldError{Field: "sql.table", Issue: fmt.Sprintf("not a valid SQL identifier: %q", table)}
	}
	if primaryKey != "" && !sqlident.ValidIdent(primaryKey) {
		return &cluster.FieldError{Field: "sql.primaryKey", Issue: fmt.Sprintf("not a valid SQL identifier: %q", primaryKey)}
	}
	if keyColumn != "" && !sqlident.ValidIdent(keyColumn) {
		return &cluster.FieldError{Field: "sql.keyColumn", Issue: fmt.Sprintf("not a valid SQL identifier: %q", keyColumn)}
	}
	for _, m := range mappings {
		if !sqlident.ValidIdent(m.Column) {
			return &cluster.FieldError{Field: "sql.mappings", Issue: fmt.Sprintf("column is not a valid SQL identifier: %q", m.Column)}
		}
		if !sqlident.ValidTypeExpr(m.SQLType) {
			return &cluster.FieldError{Field: "sql.mappings", Issue: fmt.Sprintf("column %q has an invalid SQL type %q: only letters, digits, spaces, underscores, parentheses and commas are allowed", m.Column, m.SQLType)}
		}
	}
	for _, idx := range indexes {
		if !sqlident.ValidIdent(idx.IndexName) {
			return &cluster.FieldError{Field: "sql.indexes", Issue: fmt.Sprintf("index name is not a valid SQL identifier: %q", idx.IndexName)}
		}
		for col := range strings.SplitSeq(idx.ColumnNames, ",") {
			if !sqlident.ValidIdent(strings.TrimSpace(col)) {
				return &cluster.FieldError{Field: "sql.indexes", Issue: fmt.Sprintf("index %q references an invalid column %q", idx.IndexName, strings.TrimSpace(col))}
			}
		}
	}
	return nil
}

type Config struct {
	Database cluster.Database
	// DatabaseID is the id of the [database] config Database was resolved from
	// (the sql.db value). The resolved Database handle exposes no id, so the
	// parser threads it here for identity comparison: re-pointing a syncable at a
	// different database while its SyncableIndex checkpoint is inherited by id
	// would materialize the new database only from the checkpoint forward. Empty
	// for directly-constructed configs (tests), which compare equal to each other.
	DatabaseID string
	Topic      string
	Table      string
	Mappings   []Mapping
	Indexes    []Index
	PrimaryKey string
	// KeyColumn names the column whose value equals the entity's Key, used
	// to translate a delete Actual into `DELETE FROM <table> WHERE
	// <KeyColumn> = ?`. When empty it falls back to PrimaryKey (the common
	// case: the entity Key is the row's primary key), so a config that only
	// sets primaryKey honors deletes for free. See DeleteKeyColumn.
	KeyColumn string
	// Checkpoint is the per-syncable checkpoint cadence parsed from the
	// common [syncable] section. Zero fields mean "use the default" (the
	// worker resolves them to the batch limits). For a batch syncable Every
	// is the batch size and MaxAge the batch-age flush.
	Checkpoint cluster.CheckpointPolicy
}

// DeleteKeyColumn returns the column a delete binds the entity Key against:
// KeyColumn if set, otherwise PrimaryKey. Empty means the syncable cannot
// generate a delete (neither was configured) — Init leaves the delete
// statement unprepared and Sync rejects deletes as a permanent
// misconfiguration rather than silently dropping the erasure.
func (c *Config) DeleteKeyColumn() string {
	if c.KeyColumn != "" {
		return c.KeyColumn
	}
	return c.PrimaryKey
}

type Insert struct {
	SQL      string
	Stmt     *gosql.Stmt
	JsonPath []string
}

// Delete is the prepared `DELETE FROM <table> WHERE <keyCol> = ?` statement.
// Its single placeholder binds the entity's Key, so honoring a delete needs
// no JSON payload (the delete sentinel is never unmarshaled).
type Delete struct {
	SQL  string
	Stmt *gosql.Stmt
}

// AppliedMark is the prepared dedup-sidecar mark for a KEYLESS (append) syncable:
// `INSERT ... ON CONFLICT DO NOTHING` keyed on (committed_index, committed_seq).
// It is nil for a keyed syncable, whose upsert is already idempotent. Run before
// the history insert in the same transaction; RowsAffected == 0 means the row was
// already applied (a replay), so the history insert is skipped.
type AppliedMark struct {
	SQL  string
	Stmt *gosql.Stmt
}

// Sidecar* are the fixed column names of an aggregate source's backing table.
// They are shared between projection.go (which builds the sidecar upsert and
// delete through the generic CreateSQL / CreateDeleteSQL path) and the dialects
// (which build the sidecar DDL and the materialize / rebuild / lookup SQL), so
// the two never drift.
const (
	SidecarChildKey   = "child_key"
	SidecarParentKey  = "parent_key"
	SidecarElementKey = "element_key"
	SidecarElement    = "element"
)

// AppliedIndexColumn / AppliedSeqColumn are the dedup sidecar's columns: the
// Actual's raft index and the entity's ordinal within that Actual. Together they
// are a deterministic, log-derived identity for each appended row, so replay
// re-inserts the same pair and the ON CONFLICT DO NOTHING mark reports it as
// already-applied. Shared between the dialects (which build the DDL + mark SQL)
// and sql.go (which creates/drops the sidecar), so the two never drift.
const (
	AppliedIndexColumn = "committed_index"
	AppliedSeqColumn   = "committed_seq"
)

// AppliedSidecarName is the dedup sidecar backing a keyless syncable's table —
// distinct from the projection aggregate/lookup sidecars (sidecarName).
func AppliedSidecarName(table string) string {
	return table + "__committed_applied"
}

// maxSidecarIdentifierLen is the tightest table-identifier length across the
// supported dialects (PostgreSQL 63, MySQL 64). A keyless syncable's dedup
// sidecar name must fit it; a longer name would be silently truncated by the
// database and collide with the base table, so the parser rejects it up front.
const maxSidecarIdentifierLen = 63

// Lookup* are the fixed column names of an enrichment dimension table: the
// foreign-key target and the stored JSON object of dimension fields.
const (
	LookupKey    = "lookup_key"
	LookupFields = "lookup_fields"
)

// AggregateEnrichmentField is one field an aggregate element pulls from a
// dimension: Output is the element key it lands under, Source the dimension
// field it reads.
type AggregateEnrichmentField struct {
	Output string
	Source string
}

// AggregateEnrichment is one dimension join an aggregate's materialize performs:
// LEFT JOIN Dimension on the element's OnField equals the dimension key, pulling
// each of Selects into the element. Several element fields sharing a dimension
// and on-field coalesce into one AggregateEnrichment (one join, many selects).
type AggregateEnrichment struct {
	Dimension string
	OnField   string
	Selects   []AggregateEnrichmentField
}

// AggregateSpec is the dialect-facing description of one aggregate source: the
// parent table it folds into, that table's primary-key and array columns, the
// sidecar table backing the column, whether the array orders by its element key
// numerically (1,2,…,10) rather than lexically (1,10,2), and any dimension
// enrichments joined in at materialize. The dialects use it to build the
// sidecar DDL and the materialize / rebuild / lookup / affected-parents
// statements; it carries no per-row data.
type AggregateSpec struct {
	Table       string
	PrimaryKey  string
	Column      string
	Sidecar     string
	NumericSort bool
	Enrichments []AggregateEnrichment
}

// LookupSpec is the dialect-facing description of one enrichment dimension: just
// the table that backs it (its columns are fixed — see Lookup* and
// CreateLookupDimensionDDL).
type LookupSpec struct {
	Dimension string
}
