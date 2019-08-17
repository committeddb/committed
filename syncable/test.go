package syncable

import "github.com/spf13/viper"

func testParser(v *viper.Viper, databases map[string]Database) (Syncable, error) {
	topic := v.GetString("test.topic")
	return &TestSyncable{topics: []string{topic}}, nil
}
