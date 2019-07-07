package syncable

import (
	"bytes"

	"github.com/philborlin/committed/types"
	"github.com/spf13/viper"
)

var parsers = map[string]func(*viper.Viper, map[string]types.Database) TopicSyncable{
	"sql": sqlParser,
}

// Parse parses configuration files
func Parse(style string, file []byte, dbs map[string]types.Database) (Syncable, error) {
	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(bytes.NewBuffer(file))
	if err != nil {
		return nil, err
	}

	dbType := v.GetString("db.type")

	sync := newSyncableWrapper(parsers[dbType](v, dbs))

	return sync, nil
}
