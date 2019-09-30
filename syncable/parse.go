package syncable

import (
	"fmt"
	"io"

	"github.com/spf13/viper"
)

var parsers = map[string]func(*viper.Viper, map[string]Database) (Syncable, error){
	"sql":  sqlParser,
	"test": testParser,
	"file": fileParser,
}

// ParseSyncable turns a toml file into a Syncable
func ParseSyncable(style string, reader io.Reader, dbs map[string]Database) (string, Syncable, error) {
	v, err := parseBytes(style, reader)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("syncable.name")
	dbType := v.GetString("syncable.dbType")
	parser, ok := parsers[dbType]

	if !ok {
		return "", nil, fmt.Errorf("Cannot parse database of type: %s", dbType)
	}

	syncable, err := parser(v, dbs)
	if err != nil {
		return "", nil, err
	}
	return name, syncable, nil
}

// ParseDatabase turns a toml file into a Database
func ParseDatabase(style string, reader io.Reader) (string, Database, error) {
	v, err := parseBytes(style, reader)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("database.name")
	dbType := v.GetString("database.type")

	switch dbType {
	case "sql":
		driver := v.GetString("database.sql.dialect")
		connectionString := v.GetString("database.sql.connectionString")
		db := NewSQLDB(driver, connectionString)
		return name, db, nil
	case "test":
		return name, &TestDB{}, nil
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
