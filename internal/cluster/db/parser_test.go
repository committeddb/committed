package db_test

import (
	"encoding/json"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type SyncableConfig struct {
	Details *Details `json:"syncable"`
}

type DatabaseConfig struct {
	Details *Details `json:"database"`
}

type IngestableConfig struct {
	Details *Details `json:"ingestable"`
}

type Details struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type TestDatabaseParser struct{}

func (p *TestDatabaseParser) Parse(*viper.Viper) (cluster.Database, error) {
	return &clusterfakes.FakeDatabase{}, nil
}

func TestDatabase(t *testing.T) {
	cfg1 := createDatabaseConfiguration("foo")
	cfg2 := createDatabaseConfiguration("bar")

	tests := []struct {
		configurations []*cluster.Configuration
	}{
		{[]*cluster.Configuration{cfg1}},
		{[]*cluster.Configuration{cfg1, cfg2}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			db := createDB()
			defer db.Close()

			p1 := &clusterfakes.FakeDatabaseParser{}
			p2 := &clusterfakes.FakeDatabaseParser{}
			d1 := &clusterfakes.FakeDatabase{}
			p1.ParseReturns(d1, nil)
			p2.ParseReturns(d1, nil)
			db.AddDatabaseParser("foo", p1)
			db.AddDatabaseParser("bar", p2)

			for _, cfg := range tt.configurations {
				err := db.ProposeDatabase(cfg)
				require.Equal(t, nil, err)
				<-db.CommitC
			}

			ps, err := db.ents()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.configurations), len(ps))
		})
	}
}

func TestIngestable(t *testing.T) {
	cfg1 := createIngestableConfiguration("foo")
	cfg2 := createIngestableConfiguration("bar")

	tests := []struct {
		configurations []*cluster.Configuration
	}{
		{[]*cluster.Configuration{cfg1}},
		{[]*cluster.Configuration{cfg1, cfg2}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			db := createDB()
			defer db.Close()

			p1 := &clusterfakes.FakeIngestableParser{}
			p2 := &clusterfakes.FakeIngestableParser{}
			d1 := &clusterfakes.FakeIngestable{}
			p1.ParseReturns(d1, nil)
			p2.ParseReturns(d1, nil)
			db.AddIngestableParser("foo", p1)
			db.AddIngestableParser("bar", p2)

			for _, cfg := range tt.configurations {
				err := db.ProposeIngestable(cfg)
				require.Equal(t, nil, err)
				<-db.CommitC
			}

			ps, err := db.ents()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.configurations), len(ps))
		})
	}
}

func TestSyncable(t *testing.T) {
	cfg1 := createSyncableConfiguration("foo")
	cfg2 := createSyncableConfiguration("bar")

	tests := []struct {
		configurations []*cluster.Configuration
	}{
		{[]*cluster.Configuration{cfg1}},
		{[]*cluster.Configuration{cfg1, cfg2}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			db := createDB()
			defer db.Close()

			p1 := &clusterfakes.FakeSyncableParser{}
			p2 := &clusterfakes.FakeSyncableParser{}
			db.AddSyncableParser("foo", p1)
			db.AddSyncableParser("bar", p2)

			for _, cfg := range tt.configurations {
				err := db.ProposeSyncable(cfg)
				require.Equal(t, nil, err)
				<-db.CommitC
			}

			ps, err := db.ents()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.configurations), len(ps))
		})
	}
}

func createSyncableConfiguration(name string) *cluster.Configuration {
	d := &SyncableConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}

func createDatabaseConfiguration(name string) *cluster.Configuration {
	d := &DatabaseConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}

func createIngestableConfiguration(name string) *cluster.Configuration {
	d := &IngestableConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}

func createConfiguration(id string, v any) *cluster.Configuration {
	bs, _ := json.Marshal(v)
	return &cluster.Configuration{ID: id, MimeType: "application/json", Data: bs}
}
