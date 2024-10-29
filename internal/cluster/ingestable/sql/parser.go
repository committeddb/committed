package sql

import (
	"fmt"
	"reflect"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

type IngestableParser struct{}

func (p *IngestableParser) Parse(v *viper.Viper, storage cluster.DatabaseStorage) (cluster.Ingestable, error) {
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

func (p *IngestableParser) ParseConfig(v *viper.Viper, storage cluster.DatabaseStorage) (*Config, error) {
	sqlDB := v.GetString("sql.db")
	db, err := storage.Database(sqlDB)
	if err != nil {
		return nil, err
	}

	// topic := v.GetString("sql.topic")
	// table := v.GetString("sql.table")
	// primaryKey := v.GetString("sql.primaryKey")

	// var mappings []Mapping
	// for _, item := range v.Get("sql.mappings").([]interface{}) {
	// 	m := item.(map[string]interface{})
	// 	mapping := Mapping{
	// 		JsonPath: m["jsonPath"].(string),
	// 		Column:   m["column"].(string),
	// 		SQLType:  m["type"].(string),
	// 	}
	// 	mappings = append(mappings, mapping)
	// }

	// var indexes []Index
	// for _, item := range v.Get("sql.indexes").([]interface{}) {
	// 	m := item.(map[string]interface{})
	// 	i := Index{
	// 		IndexName:   m["name"].(string),
	// 		ColumnNames: m["index"].(string),
	// 	}
	// 	indexes = append(indexes, i)
	// }

	// config := &Config{
	// 	Database:   db,
	// 	Topic:      topic,
	// 	Table:      table,
	// 	Mappings:   mappings,
	// 	Indexes:    indexes,
	// 	PrimaryKey: primaryKey,
	// }

	config := &Config{
		Database: db,
	}

	return config, nil
}
