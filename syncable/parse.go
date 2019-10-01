package syncable

import (
	"fmt"
	"io"

	"github.com/spf13/viper"
)

// Parser will parse a viper file into a Syncable
type Parser interface {
	Parse(*viper.Viper, map[string]Database) (Syncable, error)
}

// DBParser will parse a viper file into a Database
type DBParser interface {
	Parse(*viper.Viper) (Database, error)
}

var syncableParsers = map[string]Parser{}

var databaseParsers = map[string]DBParser{}

// RegisterParser registers a parser
func RegisterParser(name string, p Parser) {
	syncableParsers[name] = p
}

// RegisterDBParser registers a database parser
func RegisterDBParser(name string, p DBParser) {
	databaseParsers[name] = p
}

// ParseSyncable turns a toml file into a Syncable
func ParseSyncable(style string, reader io.Reader, dbs map[string]Database) (string, Syncable, error) {
	v, err := parseBytes(style, reader)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("syncable.name")
	dbType := v.GetString("syncable.dbType")
	parser, ok := syncableParsers[dbType]

	if !ok {
		return "", nil, fmt.Errorf("Cannot parse syncable of type: %s", dbType)
	}

	syncable, err := parser.Parse(v, dbs)
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
	parser, ok := databaseParsers[dbType]

	if !ok {
		return "", nil, fmt.Errorf("Cannot parse database of type: %s", dbType)
	}

	database, err := parser.Parse(v)
	if err != nil {
		return "", nil, err
	}
	return name, database, nil
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
