package sql

import (
	"fmt"
	"reflect"

	"github.com/spf13/viper"

	"github.com/philborlin/committed/internal/cluster"
)

type SyncableParser struct{}

func (p *SyncableParser) Parse(v *viper.Viper, storage cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v, storage)
	if err != nil {
		return nil, err
	}

	db, ok := config.Database.(*DB)
	if !ok {
		return nil, fmt.Errorf("expected sql.DB but was %s", reflect.TypeOf(config.Database))
	}

	syncable := New(db, config)
	err = syncable.Init()
	if err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] init: %w", err)
	}

	return syncable, nil
}

func (p *SyncableParser) ParseConfig(v *viper.Viper, storage cluster.DatabaseStorage) (*Config, error) {
	sqlDB := v.GetString("sql.db")
	db, err := storage.Database(sqlDB)
	if err != nil {
		return nil, err
	}

	topic := v.GetString("sql.topic")
	table := v.GetString("sql.table")
	primaryKey := v.GetString("sql.primaryKey")

	var mappings []Mapping
	if err := v.UnmarshalKey("sql.mappings", &mappings); err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] parse sql.mappings: %w", err)
	}

	var indexes []Index
	if err := v.UnmarshalKey("sql.indexes", &indexes); err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] parse sql.indexes: %w", err)
	}

	config := &Config{
		Database:   db,
		Topic:      topic,
		Table:      table,
		Mappings:   mappings,
		Indexes:    indexes,
		PrimaryKey: primaryKey,
	}

	return config, nil
}
