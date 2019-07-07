package syncable

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/philborlin/committed/types"
	"github.com/spf13/viper"
)

func TestParseWithSQLToml(t *testing.T) {
	dat, err := ioutil.ReadFile("./simple.toml")
	if err != nil {
		t.Fatalf("Failed with error %v", err)
	}

	parsed, _ := Parse("toml", dat, databases())
	sqlSyncable := parsed.(*syncableWrapper).Syncable.(*SQLSyncable)

	actual := sqlSyncable.config
	expected := simpleConfig()

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected %v but was %v", expected, actual)
	}
}

func TestSQLParser(t *testing.T) {
	dat, err := ioutil.ReadFile("./simple.toml")
	if err != nil {
		t.Fatalf("Failed with error %v", err)
	}

	var v = viper.New()

	v.SetConfigType("toml")
	v.ReadConfig(bytes.NewBuffer(dat))

	actual := sqlParser(v, databases()).(*SQLSyncable).config
	expected := simpleConfig()

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected %v but was %v", expected, actual)
	}
}

func simpleConfig() sqlConfig {
	m1 := sqlMapping{"$.Key", "foo", "key"}
	m2 := sqlMapping{"$.One", "foo", "two"}
	m := []sqlMapping{m1, m2}
	// return sqlConfig{"ql", "memory://foo", "test1", m}
	return sqlConfig{"testdb", "test1", m}
}

func databases() map[string]types.Database {
	sqlDB := types.NewSQLDB("foo", "ql", "memory://foo")
	err := sqlDB.Init()
	if err != nil {
		return nil
	}
	m := make(map[string]types.Database)
	m["testdb"] = sqlDB
	return m
}
