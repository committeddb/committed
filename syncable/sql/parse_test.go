package sql

import (
	"bytes"
	"io/ioutil"

	"github.com/philborlin/committed/syncable"
	"github.com/spf13/viper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Syncable Parser", func() {
	Describe("Simple toml", func() {
		var (
			data []byte
			err  error
			dbs  map[string]syncable.Database
		)

		JustBeforeEach(func() {
			data, err = ioutil.ReadFile("./simple.toml")
			Expect(err).To(BeNil())
			dbs, err = databases()
			Expect(err).To(BeNil())
		})

		JustAfterEach(func() {
			dbs["testdb"].Close()
		})

		It("should parse with SQL toml", func() {
			name, parsed, err := syncable.ParseSyncable("toml", bytes.NewReader(data), dbs)
			Expect(err).To(BeNil())
			Expect(name).To(Equal("foo"))

			// wrapper := parsed.(*syncableWrapper)
			// sqlSyncable := wrapper.Syncable.(*SQLSyncable)
			syncable := parsed.(*Syncable)

			actual := syncable.config
			expected := simpleConfig()

			Expect(actual).To(Equal(expected))
		})

		It("should parse with sql parser", func() {
			var v = viper.New()
			v.SetConfigType("toml")
			v.ReadConfig(bytes.NewBuffer(data))

			topicSyncable, err := sqlParser(v, dbs)
			Expect(err).To(BeNil())

			actual := topicSyncable.(*Syncable).config
			expected := simpleConfig()

			Expect(actual).To(Equal(expected))
		})
	})
})

func simpleConfig() *sqlConfig {
	m1 := sqlMapping{jsonPath: "$.Key", column: "pk", sqlType: "TEXT"}
	m2 := sqlMapping{jsonPath: "$.One", column: "one", sqlType: "TEXT"}
	m := []sqlMapping{m1, m2}

	i1 := index{indexName: "firstIndex", columnNames: "one"}
	i := []index{i1}
	return &sqlConfig{sqlDB: "testdb", topic: "test1", table: "foo", mappings: m, indexes: i, primaryKey: "pk"}
}

func databases() (map[string]syncable.Database, error) {
	sqlDB := NewDB("ramsql", "memory://foo")
	err := sqlDB.Init()
	if err != nil {
		return nil, err
	}
	m := make(map[string]syncable.Database)
	m["testdb"] = sqlDB
	return m, nil
}
