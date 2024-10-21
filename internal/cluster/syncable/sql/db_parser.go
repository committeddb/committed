package sql

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

type DBParser struct {
	Dialects map[string]Dialect
}

func (d *DBParser) Parse(v *viper.Viper, name string) (cluster.Database, error) {
	dialectName := v.GetString("sql.dialect")
	connectionString := v.GetString("sql.connectionString")

	dialect, ok := d.Dialects[dialectName]
	if !ok {
		return nil, fmt.Errorf("%s not found", dialectName)
	}

	db, err := NewDB(dialect, connectionString)
	if err != nil {
		return nil, err
	}

	return db, nil
}
