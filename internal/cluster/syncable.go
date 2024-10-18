package cluster

import (
	"context"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

type ShouldSnapshot bool

//counterfeiter:generate . Syncable
type Syncable interface {
	Sync(ctx context.Context, p *Proposal) (ShouldSnapshot, error)
	Close() error
}

// Parser will parse a viper file into a Syncable
//
//counterfeiter:generate . SyncableParser
type SyncableParser interface {
	Parse(*viper.Viper, DatabaseStorage) (Syncable, error)
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

var syncableIndexType = &Type{
	ID:      "ab972bba-83fe-4dea-9c5d-877645e8d21e",
	Name:    "InternalSyncableIndex",
	Version: 1,
}

type SyncableIndex struct {
	ID    string
	Index uint64
}

func (i *SyncableIndex) Marshal() ([]byte, error) {
	li := &clusterpb.LogSyncableIndex{ID: i.ID, Index: i.Index}
	return proto.Marshal(li)
}

func (i *SyncableIndex) Unmarshal(bs []byte) error {
	li := &clusterpb.LogSyncableIndex{}
	err := proto.Unmarshal(bs, li)
	if err != nil {
		return err
	}

	i.ID = li.ID
	i.Index = li.Index

	return nil
}

func IsSyncableIndex(id string) bool {
	return id == syncableIndexType.ID
}

func NewUpsertSyncableIndexEntity(i *SyncableIndex) (*Entity, error) {
	bs, err := i.Marshal()
	if err != nil {
		return nil, err
	}

	// TODO Hardcoded
	return NewUpsertEntity(syncableIndexType, []byte("foo"), bs), nil
}
