package sql

import (
	"github.com/committeddb/committed/internal/cluster"
)

// databaseSQLKeys is the database-config [sql] vocabulary (Parse's reads).
var databaseSQLKeys = []string{"dialect", "connectionString"}

type DBParser struct {
	Dialects map[string]Dialect
}

func (d *DBParser) Parse(v *cluster.ParsedConfig) (cluster.Database, error) {
	if err := v.RejectUnknownKeys("sql", databaseSQLKeys...); err != nil {
		return nil, err
	}
	dialectName := v.GetString("sql.dialect")
	connectionString := v.GetString("sql.connectionString")

	dialect, ok := d.Dialects[dialectName]
	if !ok {
		return nil, cluster.UnknownDialectError(dialectName, dialectNames(d.Dialects))
	}

	db, err := NewDB(dialect, connectionString)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// dialectNames returns the registered dialect names, for the "valid: ..." list
// in an unknown-dialect error. Shared by the syncable database parser here and
// the syncable parser.
func dialectNames(m map[string]Dialect) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return names
}
