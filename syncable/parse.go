package syncable

import (
	"bytes"

	"github.com/spf13/viper"
)

var parsers = map[string]func(*viper.Viper) TopicSyncable{
	"sql": sqlParser,
}

// Parse parses configuration files
func Parse(style string, file []byte) (Syncable, error) {
	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(bytes.NewBuffer(file))
	if err != nil {
		return nil, err
	}

	dbType := v.GetString("db.type")

	sync := newSyncableWrapper(parsers[dbType](v))

	return sync, nil
}
