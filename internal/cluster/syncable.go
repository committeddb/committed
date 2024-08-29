package cluster

import (
	"context"

	"github.com/spf13/viper"
)

//counterfeiter:generate . Syncable
type Syncable interface {
	Sync(ctx context.Context, p *Proposal) error
	Close() error
}

// Parser will parse a viper file into a Syncable
//
//counterfeiter:generate . SyncableParser
type SyncableParser interface {
	Parse(*viper.Viper, map[string]Database) (Syncable, error)
}

var syncableType = &Type{
	ID:      "0cd18065-a0e2-4c19-a4d6-f824f1898cb5",
	Name:    "InternalSyncableParser",
	Version: 1,
}

func IsSyncable(id string) bool {
	return id == syncableType.ID
}

func NewUpsertSyncableEntity(c *Configuration) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(syncableType, []byte(c.ID), bs), nil
}
