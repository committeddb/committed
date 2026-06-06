package sql

import (
	gosql "database/sql"

	"github.com/philborlin/committed/internal/cluster"
)

type Dialect interface {
	CreateDDL(config *Config) string
	CreateSQL(config *Config) string
	// CreateDeleteSQL returns the statement that removes one downstream row
	// by its key column: `DELETE FROM <table> WHERE <keyCol> = <placeholder>`.
	// The placeholder is dialect-specific (? for MySQL, $1 for PostgreSQL).
	// The key column is Config.DeleteKeyColumn(); the single bound argument
	// is the entity's Key, so deletes never unmarshal the (absent) payload.
	CreateDeleteSQL(config *Config) string
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
	// KeyColumn names the column whose value equals the entity's Key, used
	// to translate a delete Actual into `DELETE FROM <table> WHERE
	// <KeyColumn> = ?`. When empty it falls back to PrimaryKey (the common
	// case: the entity Key is the row's primary key), so a config that only
	// sets primaryKey honors deletes for free. See DeleteKeyColumn.
	KeyColumn string
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
