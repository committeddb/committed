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
}

type Config struct {
	ConnectionString string
	Type             *cluster.Type
	Mappings         []Mapping
	PrimaryKey       string
	Tables           []string
	Options          map[string]string
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
