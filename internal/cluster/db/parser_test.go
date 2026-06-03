package db_test

import (
	"encoding/json"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
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
				err := db.ProposeDatabase(testCtx(t), cfg)
				require.Equal(t, nil, err)
			}

			ps, err := db.ents()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.configurations), len(ps))
		})
	}
}

// TestProposeDatabase_PersistsName verifies the full name path: ProposeDatabase
// parses the config, extracts database.name, records it on the Configuration,
// and that name survives marshaling into the stored entity. createDatabaseConfiguration
// sets database.name == database.type == "foo".
func TestProposeDatabase_PersistsName(t *testing.T) {
	db := createDB()
	defer db.Close()

	p := &clusterfakes.FakeDatabaseParser{}
	p.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	db.AddDatabaseParser("foo", p)

	cfg := createDatabaseConfiguration("foo")
	require.Equal(t, nil, db.ProposeDatabase(testCtx(t), cfg))

	ps, err := db.ents()
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(ps))
	require.Equal(t, 1, len(ps[0].Entities))

	var got cluster.Configuration
	require.Equal(t, nil, got.Unmarshal(ps[0].Entities[0].Data))
	require.Equal(t, "foo", got.Name)
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
				err := db.ProposeIngestable(testCtx(t), cfg)
				require.Equal(t, nil, err)
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
				err := db.ProposeSyncable(testCtx(t), cfg)
				require.Equal(t, nil, err)
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
