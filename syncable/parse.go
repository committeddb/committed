package syncable

import (
	"bytes"

	"github.com/spf13/viper"
)

var parsers = map[string]func(*viper.Viper) Syncable{
	"sql": sqlParser,
}

// Parse parses configuration files
func Parse(style string, file []byte) Syncable {
	var v = viper.New()

	v.SetConfigType(style)
	v.ReadConfig(bytes.NewBuffer(file))

	dbType := v.GetString("db.type")
	return parsers[dbType](v)
}

func sqlParser(v *viper.Viper) Syncable {
	driver := v.GetString("sql.driver")
	connectionString := v.GetString("sql.connectionString")
	topic := v.GetString("sql.topic.name")

	var mappings []sqlMapping
	for _, item := range v.Get("sql.topic.mapping").([]interface{}) {
		m := item.(map[string]interface{})
		mapping := sqlMapping{m["jsonPath"].(string), m["table"].(string), m["column"].(string)}
		mappings = append(mappings, mapping)
	}

	config := sqlConfig{driver, connectionString, topic, mappings}
	return newSQLSyncable(config)
}
