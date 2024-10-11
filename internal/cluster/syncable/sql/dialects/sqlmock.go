package dialects

import (
	"fmt"
	"strings"

	gosql "database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/philborlin/committed/internal/cluster/syncable/sql"
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

func (d *SQLMockDialect) CreateSQL(table string, sqlMappings []sql.Mapping) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", table)
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.Column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.Column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range sqlMappings {
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
