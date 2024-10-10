package sql

import (
	"io"

	"github.com/spf13/viper"
)

func Parse(configType string, r io.Reader) *Config {
	v := *viper.New()
	v.SetConfigType(configType)
	// TODO What should we do?
	_ = v.ReadConfig(r)

	topic := v.GetString("sql.topic")
	sqlDB := v.GetString("sql.db")
	table := v.GetString("sql.table")
	primaryKey := v.GetString("sql.primaryKey")

	var mappings []Mapping
	for _, item := range v.Get("sql.mappings").([]interface{}) {
		m := item.(map[string]interface{})
		mapping := Mapping{
			JsonPath: m["jsonPath"].(string),
			Column:   m["column"].(string),
			SQLType:  m["type"].(string),
		}
		mappings = append(mappings, mapping)
	}

	var indexes []Index
	for _, item := range v.Get("sql.indexes").([]interface{}) {
		m := item.(map[string]interface{})
		i := Index{
			IndexName:   m["name"].(string),
			ColumnNames: m["index"].(string),
		}
		indexes = append(indexes, i)
	}

	config := &Config{
		SQLDB:      sqlDB,
		Topic:      topic,
		Table:      table,
		Mappings:   mappings,
		Indexes:    indexes,
		PrimaryKey: primaryKey,
	}
	// return newSyncable(config, databases)

	return config
}
