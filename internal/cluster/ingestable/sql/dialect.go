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
}

type Config struct {
	ConnectionString string
	Type             *cluster.Type
	Mappings         []Mapping
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
