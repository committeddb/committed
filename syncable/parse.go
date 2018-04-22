package syncable

import (
	"bytes"

	"github.com/spf13/viper"
)

var parsers = map[string]func(*viper.Viper) TopicSyncable{
	"sql": sqlParser,
}

// Parse parses configuration files
func Parse(style string, file []byte) (TopicSyncable, error) {
	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(bytes.NewBuffer(file))
	if err != nil {
		return nil, err
	}

	dbType := v.GetString("db.type")
	return parsers[dbType](v), nil
}

func sqlParser(v *viper.Viper) TopicSyncable {
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
