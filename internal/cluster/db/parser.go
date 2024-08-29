package db

import (
	"bytes"
	"fmt"
	"io"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

var syncableParsers = map[string]cluster.SyncableParser{}

var databaseParsers = map[string]cluster.DatabaseParser{}

func (db *DB) AddSyncableParser(name string, p cluster.SyncableParser) {
	syncableParsers[name] = p
}

func (db *DB) AddDatabaseParser(name string, p cluster.DatabaseParser) {
	databaseParsers[name] = p
}

func (db *DB) ProposeSyncable(c *cluster.Configuration) error {
	_, _, err := parseSyncable(c.MimeType, bytes.NewReader(c.Data), db.storage)
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertSyncableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	db.Propose(p)

	return nil
}
func (db *DB) ProposeDatabase(c *cluster.Configuration) error {
	_, _, err := parseDatabase(c.MimeType, bytes.NewReader(c.Data))
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertDatabaseEntity(c)
	if err != nil {
		return err
	}
	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	db.Propose(p)

	return nil
}

func parseSyncable(mimeType string, reader io.Reader, s cluster.DatabaseStorage) (string, cluster.Syncable, error) {
	v, err := parseBytes(mimeType, reader)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("syncable.name")
	tipe := v.GetString("syncable.type")
	parser, ok := syncableParsers[tipe]

	if !ok {
		return "", nil, fmt.Errorf("cannot parse syncable of type: %s", tipe)
	}

	syncable, err := parser.Parse(v, s)
	if err != nil {
		return "", nil, err
	}
	return name, syncable, nil
}

func parseDatabase(mimeType string, reader io.Reader) (string, cluster.Database, error) {
	v, err := parseBytes(mimeType, reader)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("database.name")
	dbType := v.GetString("database.type")
	parser, ok := databaseParsers[dbType]

	if !ok {
		return "", nil, fmt.Errorf("cannot parse database of type: %s", dbType)
	}

	database, err := parser.Parse(v)
	if err != nil {
		return "", nil, err
	}
	return name, database, nil
}

func parseBytes(mimeType string, reader io.Reader) (*viper.Viper, error) {
	style := "toml"
	if mimeType == "application/json" {
		style = "json"
	}

	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(reader)
	if err != nil {
		return nil, err
	}

	return v, nil
}
