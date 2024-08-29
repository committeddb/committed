package cluster

import "github.com/spf13/viper"

// Database represents a query database
//
//counterfeiter:generate . Database
type Database interface {
	Close() error
}

type DatabaseStorage interface {
	Database(id string) (Database, error)
}

// DatabaseParser will parse a viper file into a Database
//
//counterfeiter:generate . DatabaseParser
type DatabaseParser interface {
	Parse(*viper.Viper) (Database, error)
}

var databaseType = &Type{
	ID:      "4698b77e-9a7c-41a2-aae4-984da0cd33c1",
	Name:    "InternalDatabaseParser",
	Version: 1,
}

func IsDatabase(id string) bool {
	return id == databaseType.ID
}

func NewUpsertDatabaseEntity(c *Configuration) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(databaseType, []byte(c.ID), bs), nil
}
