package dialects

import (
	"fmt"
	"strings"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

func createDDL(config *sql.Config) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", config.Table))
	for i, column := range config.Mappings {
		ddl.WriteString(fmt.Sprintf("%s %s", column.Column, column.SQLType))
		if i < len(config.Mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if config.PrimaryKey != "" {
		ddl.WriteString(fmt.Sprintf(",PRIMARY KEY (%s)", config.PrimaryKey))
	}
	for _, index := range config.Indexes {
		ddl.WriteString(fmt.Sprintf(",INDEX %s (%s)", index.IndexName, index.ColumnNames))
	}
	ddl.WriteString(");")

	return ddl.String()
}
