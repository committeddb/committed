package cluster

// Database represents a query database
//
//counterfeiter:generate . Database
type Database interface {
	Close() error
	GetType() string
}

type DatabaseStorage interface {
	Database(id string) (Database, error)
}

// DatabaseParser will parse a config document into a Database
//
//counterfeiter:generate . DatabaseParser
type DatabaseParser interface {
	Parse(c *ParsedConfig) (Database, error)
}

// EntityKindRevision (version-stored config with rollback, retained — see
// typeType).
var databaseType = registerSystemType(&Type{
	ID:         "4698b77e-9a7c-41a2-aae4-984da0cd33c1",
	Name:       "InternalDatabaseParser",
	Version:    1,
	EntityKind: EntityKindRevision,
})

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
