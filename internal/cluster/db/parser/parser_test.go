package parser_test

import (
	"fmt"
	"testing"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/stretchr/testify/require"
)

// --- ParseDatabase ---

func TestParseDatabase_TOML_Success(t *testing.T) {
	p := parser.New()

	fakeDB := &clusterfakes.FakeDatabase{}
	fakeParser := &clusterfakes.FakeDatabaseParser{}
	fakeParser.ParseReturns(fakeDB, nil)
	p.AddDatabaseParser("sql", fakeParser)

	data := []byte(`[database]
type = "sql"
name = "mydb"
[sql]
dialect = "mysql"`)

	name, db, err := p.ParseDatabase("text/toml", data)
	require.Nil(t, err)
	require.Equal(t, "mydb", name)
	require.Equal(t, fakeDB, db)
	require.Equal(t, 1, fakeParser.ParseCallCount())

	v, parsedName := fakeParser.ParseArgsForCall(0)
	require.Equal(t, "mydb", parsedName)
	require.Equal(t, "mysql", v.GetString("sql.dialect"))
}

func TestParseDatabase_JSON_Success(t *testing.T) {
	p := parser.New()

	fakeDB := &clusterfakes.FakeDatabase{}
	fakeParser := &clusterfakes.FakeDatabaseParser{}
	fakeParser.ParseReturns(fakeDB, nil)
	p.AddDatabaseParser("sql", fakeParser)

	data := []byte(`{"database": {"type": "sql", "name": "jsondb"}, "sql": {"dialect": "postgres"}}`)

	name, db, err := p.ParseDatabase("application/json", data)
	require.Nil(t, err)
	require.Equal(t, "jsondb", name)
	require.Equal(t, fakeDB, db)

	v, _ := fakeParser.ParseArgsForCall(0)
	require.Equal(t, "postgres", v.GetString("sql.dialect"))
}

func TestParseDatabase_UnknownType(t *testing.T) {
	p := parser.New()

	data := []byte(`[database]
type = "unknown"
name = "mydb"`)

	_, _, err := p.ParseDatabase("text/toml", data)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "cannot parse database of type: unknown")
}

func TestParseDatabase_SubParserError(t *testing.T) {
	p := parser.New()

	fakeParser := &clusterfakes.FakeDatabaseParser{}
	fakeParser.ParseReturns(nil, fmt.Errorf("connection refused"))
	p.AddDatabaseParser("sql", fakeParser)

	data := []byte(`[database]
type = "sql"
name = "mydb"`)

	_, _, err := p.ParseDatabase("text/toml", data)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func TestParseDatabase_MalformedTOML(t *testing.T) {
	p := parser.New()

	data := []byte(`this is not valid toml {{{`)

	_, _, err := p.ParseDatabase("text/toml", data)
	require.NotNil(t, err)
}

// --- ParseIngestable ---

func TestParseIngestable_Success(t *testing.T) {
	p := parser.New()

	fakeIngestable := &clusterfakes.FakeIngestable{}
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(fakeIngestable, nil)
	p.AddIngestableParser("kafka", fakeParser)

	data := []byte(`[ingestable]
type = "kafka"
name = "my-ingest"`)

	name, ingestable, err := p.ParseIngestable("text/toml", data)
	require.Nil(t, err)
	require.Equal(t, "my-ingest", name)
	require.Equal(t, fakeIngestable, ingestable)
	require.Equal(t, 1, fakeParser.ParseCallCount())
}

func TestParseIngestable_UnknownType(t *testing.T) {
	p := parser.New()

	data := []byte(`[ingestable]
type = "unknown"
name = "my-ingest"`)

	_, _, err := p.ParseIngestable("text/toml", data)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "cannot parse ingestable of type: unknown")
}

func TestParseIngestable_SubParserError(t *testing.T) {
	p := parser.New()

	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(nil, fmt.Errorf("parse failed"))
	p.AddIngestableParser("kafka", fakeParser)

	data := []byte(`[ingestable]
type = "kafka"
name = "my-ingest"`)

	_, _, err := p.ParseIngestable("text/toml", data)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "parse failed")
}

// --- ParseSyncable ---

func TestParseSyncable_Success(t *testing.T) {
	p := parser.New()

	fakeSyncable := &clusterfakes.FakeSyncable{}
	fakeParser := &clusterfakes.FakeSyncableParser{}
	fakeParser.ParseReturns(fakeSyncable, nil)
	p.AddSyncableParser("sql", fakeParser)

	data := []byte(`[syncable]
type = "sql"
name = "my-sync"`)

	name, syncable, err := p.ParseSyncable("text/toml", data, nil)
	require.Nil(t, err)
	require.Equal(t, "my-sync", name)
	require.Equal(t, fakeSyncable, syncable)
	require.Equal(t, 1, fakeParser.ParseCallCount())
}

func TestParseSyncable_UnknownType(t *testing.T) {
	p := parser.New()

	data := []byte(`[syncable]
type = "unknown"
name = "my-sync"`)

	_, _, err := p.ParseSyncable("text/toml", data, nil)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "cannot parse syncable of type: unknown")
}

// --- Multiple parsers ---

func TestParser_MultipleParsers(t *testing.T) {
	p := parser.New()

	fakeDB1 := &clusterfakes.FakeDatabase{}
	fakeDB2 := &clusterfakes.FakeDatabase{}
	fakeParser1 := &clusterfakes.FakeDatabaseParser{}
	fakeParser1.ParseReturns(fakeDB1, nil)
	fakeParser2 := &clusterfakes.FakeDatabaseParser{}
	fakeParser2.ParseReturns(fakeDB2, nil)

	p.AddDatabaseParser("sql", fakeParser1)
	p.AddDatabaseParser("nosql", fakeParser2)

	data1 := []byte(`[database]
type = "sql"
name = "db1"`)

	data2 := []byte(`[database]
type = "nosql"
name = "db2"`)

	_, db1, err := p.ParseDatabase("text/toml", data1)
	require.Nil(t, err)
	require.Equal(t, fakeDB1, db1)

	_, db2, err := p.ParseDatabase("text/toml", data2)
	require.Nil(t, err)
	require.Equal(t, fakeDB2, db2)

	require.Equal(t, 1, fakeParser1.ParseCallCount())
	require.Equal(t, 1, fakeParser2.ParseCallCount())
}
