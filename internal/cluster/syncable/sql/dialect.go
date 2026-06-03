package sql

import (
	gosql "database/sql"

	"github.com/philborlin/committed/internal/cluster"
)

type Dialect interface {
	CreateDDL(config *Config) string
	CreateSQL(config *Config) string
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

type Config struct {
	Database   cluster.Database
	Topic      string
	Table      string
	Mappings   []Mapping
	Indexes    []Index
	PrimaryKey string
}

type Insert struct {
	SQL      string
	Stmt     *gosql.Stmt
	JsonPath []string
}
