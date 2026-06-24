package dialects

import (
	"fmt"
	"strings"

	gosql "database/sql"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type SQLMockDialect struct {
	db *gosql.DB
}

func NewSQLMockDialect() (*SQLMockDialect, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		return nil, nil, err
	}

	return &SQLMockDialect{db: db}, mock, nil
}

func (d *SQLMockDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

func (d *SQLMockDialect) DropDDL(c *sql.Config) string {
	return dropDDL(c)
}

// CreateDeleteSQL implements Dialect, mirroring MySQL's ? placeholder (the
// dialect the mock stands in for).
func (d *SQLMockDialect) CreateDeleteSQL(c *sql.Config) string {
	return createDeleteSQL(c, "?")
}

// CreateClearSQL implements Dialect, mirroring MySQL's ? placeholder.
func (d *SQLMockDialect) CreateClearSQL(c *sql.Config, columns []string) string {
	return createClearSQL(c, columns, "?")
}

// The aggregate builders mirror MySQL (the dialect the mock stands in for); the
// projection unit tests assert exec/prepare SQL through this same dialect, with
// real-database behavior validated against PostgreSQL in the docker tests.
func (d *SQLMockDialect) CreateAggregateSidecarDDL(spec sql.AggregateSpec) string {
	return (&MySQLDialect{}).CreateAggregateSidecarDDL(spec)
}

func (d *SQLMockDialect) CreateAggregateMaterializeSQL(spec sql.AggregateSpec) string {
	return (&MySQLDialect{}).CreateAggregateMaterializeSQL(spec)
}

func (d *SQLMockDialect) CreateAggregateRebuildSQL(spec sql.AggregateSpec) string {
	return (&MySQLDialect{}).CreateAggregateRebuildSQL(spec)
}

func (d *SQLMockDialect) CreateAggregateParentLookupSQL(spec sql.AggregateSpec) string {
	return (&MySQLDialect{}).CreateAggregateParentLookupSQL(spec)
}

func (d *SQLMockDialect) CreateLookupDimensionDDL(spec sql.LookupSpec) string {
	return (&MySQLDialect{}).CreateLookupDimensionDDL(spec)
}

func (d *SQLMockDialect) CreateAggregateAffectedParentsSQL(spec sql.AggregateSpec, onField string) string {
	return (&MySQLDialect{}).CreateAggregateAffectedParentsSQL(spec, onField)
}

func (d *SQLMockDialect) CreateSQL(config *sql.Config) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", config.Table)
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.Column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.Column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range config.Mappings {
		if i == 0 {
			fmt.Fprint(&sql, "?")
		} else {
			fmt.Fprint(&sql, ",?")
		}
	}
	fmt.Fprint(&sql, ")")

	return sql.String()
}

func (d *SQLMockDialect) Open(connectionString string) (*gosql.DB, error) {
	return d.db, nil
}

func (d *SQLMockDialect) IsPermanent(err error) bool {
	return false
}

// BindArgs doubles the values to mirror MySQL, which is the dialect the
// mock stands in for in sql_test.go (the mock does not validate arg vs
// placeholder counts, so the doubling is what the tests assert against).
func (d *SQLMockDialect) BindArgs(values []any) []any {
	return append(values, values...)
}
