package sql

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

//go:generate protoc --go_out=paths=source_relative:. ./dialectpb/dialect.proto

// Dialect is the per-source implementation behind a SQL Ingestable (Postgres
// logical replication, MySQL binlog). Ingest streams source changes as
// Proposals.
//
// Like any Ingestable, a Dialect MUST translate a source DELETE into a delete
// entity (cluster.NewDeleteEntity) keyed by the row's primary key — never an
// upsert of the deleted row's pre-image. Emitting deletes is mandatory for a
// well-behaved ingestable: only a delete entity makes the downstream Syncable
// remove the record. See the cluster.Ingestable contract.
type Dialect interface {
	Ingest(ctx context.Context, config *Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error
	// Preflight validates that the source can be ingested safely, before any
	// worker starts. Today it is the replica-identity / binlog-row-image guard:
	// it connects to the source and verifies every watched table's DELETE change
	// record carries the configured primaryKey, so deletes can't be silently
	// dropped (see CheckKeyCoverage). A non-nil error fails the ingestable's
	// build — surfaced as a degraded config (loud, queryable), and no worker is
	// started — rather than letting it run and quietly lose deletes. It manages
	// its own (short) connection timeout.
	Preflight(config *Config) error
	// Status decodes pos into a point-in-time IngestableStatus — phase, per-table
	// snapshot progress, and the CDC cursor — and, where the dialect supports it,
	// queries the source for replication lag. Read-only and side-effect free; it
	// manages its own short connection timeout for any source query and tolerates
	// the empty position (a worker that has not checkpointed yet). A source-query
	// failure leaves Lag nil rather than failing the whole status.
	Status(ctx context.Context, config *Config, pos cluster.Position) (cluster.IngestableStatus, error)
	// SourceColumns returns the column names of each watched table (keyed by the
	// table name as configured), in source order, for expanding a MapAllColumns
	// config into explicit mappings at build time. Read-only schema
	// introspection; it manages its own short connection timeout. Only called
	// when MapAllColumns is set.
	SourceColumns(config *Config) (map[string][]string, error)
}

type Config struct {
	ConnectionString string
	Type             *cluster.Type
	Mappings         []Mapping
	// MapAllColumns infers a jsonName=column mapping for every source column
	// (across all watched Tables) so a whole-table mirror needs no per-column
	// [[sql.mappings]]. The column set is FROZEN at config-build time: the
	// parser introspects the source once and expands MapAllColumns into explicit
	// Mappings, so a column added to the source later does not silently enter
	// payloads until the config is re-POSTed. Any explicit Mappings override the
	// inferred mapping for that column (a rename); ExcludeColumns drops columns
	// from the inferred set. Once the parser has expanded it, Mappings is the
	// full explicit set and the runtime never sees MapAllColumns.
	MapAllColumns bool
	// ExcludeColumns are source columns to omit from the MapAllColumns set
	// (secrets, large blobs). Only meaningful with MapAllColumns. Column names
	// are matched as the source reports them.
	ExcludeColumns []string
	// PrimaryKey is the source table's primary-key column(s). A single column
	// keys each entity by its bare value; multiple columns (a composite PK, e.g.
	// IMDb principals' (tconst, ordering)) key by all of them so rows sharing a
	// leading column don't collide. See CompositeKey.
	PrimaryKey []string
	Tables     []string
	Options    map[string]string
}

// The mapstructure tags drive viper.UnmarshalKey when parsing the
// [[sql.mappings]] array-of-tables. Required because the Go field names
// differ from the TOML keys (JsonName→jsonName, SQLColumn→column), and
// they keep parsing independent of viper's key-case handling, which
// changed between viper versions.
type Mapping struct {
	JsonName  string `mapstructure:"jsonName"`
	SQLColumn string `mapstructure:"column"`
}
