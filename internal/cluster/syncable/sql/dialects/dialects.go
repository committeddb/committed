package dialects

import (
	"fmt"
	"strings"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

func createDDL(config *sql.Config) string {
	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE IF NOT EXISTS %s (", config.Table)
	for i, column := range config.Mappings {
		fmt.Fprintf(&ddl, "%s %s", column.Column, column.SQLType)
		if i < len(config.Mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if config.PrimaryKey != "" {
		fmt.Fprintf(&ddl, ",PRIMARY KEY (%s)", config.PrimaryKey)
	}
	for _, index := range config.Indexes {
		fmt.Fprintf(&ddl, ",INDEX %s (%s)", index.IndexName, index.ColumnNames)
	}
	ddl.WriteString(");")

	return ddl.String()
}
