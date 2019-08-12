package syncable

import (
	"fmt"
	"io"

	"github.com/philborlin/committed/types"
	"github.com/spf13/viper"
)

var parsers = map[string]func(*viper.Viper, map[string]types.Database) (Syncable, error){
	"sql": sqlParser,
}

// ParseSyncable turns a toml file into a Syncable
func ParseSyncable(style string, reader io.Reader, dbs map[string]types.Database) (name string, syncable Syncable, err error) {
	v, err := parseBytes(style, reader)
	if err != nil {
		return "", nil, err
	}

	name = v.GetString("syncable.name")
	dbType := v.GetString("syncable.dbType")
	switch dbType {
	case "sql":
		syncable, err := sqlParser(v, dbs)
		if err != nil {
			return "", nil, err
		}
		return name, syncable, nil
	default:
		return "", nil, fmt.Errorf("Cannot parse database of type: %s", dbType)
	}
}

// ParseDatabase turns a toml file into a Database
func ParseDatabase(style string, reader io.Reader) (name string, db types.Database, err error) {
	v, err := parseBytes(style, reader)
	if err != nil {
		return "", nil, err
	}

	name = v.GetString("database.name")
	dbType := v.GetString("database.type")

	switch dbType {
	case "sql":
		driver := v.GetString("database.sql.dialect")
		connectionString := v.GetString("database.sql.connectionString")
		db := types.NewSQLDB(driver, connectionString)
		return name, db, nil
	default:
		return "", nil, fmt.Errorf("Cannot parse database of type: %s", dbType)
	}
}

func parseBytes(style string, reader io.Reader) (*viper.Viper, error) {
	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(reader)
	if err != nil {
		return nil, err
	}

	return v, nil
}
