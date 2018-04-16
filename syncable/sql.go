package syncable

import (
	"context"
	"database/sql"
	"log"

	// The driver will be loaded through reflection
	_ "github.com/lib/pq"
)

type sqlMapping struct {
	jsonPath string
	table    string
	column   string
}

type sqlConfig struct {
	driver           string
	connectionString string
	topic            string
	mappings         []sqlMapping
}

type sqlSyncable struct {
	config sqlConfig
	db     *sql.DB
}

// NewSQLSyncable creates a new syncable
func newSQLSyncable(sqlConfig sqlConfig) *sqlSyncable {
	db, err := sql.Open("postgres", sqlConfig.connectionString)
	if err != nil {
		log.Fatal(err)
	}

	return &sqlSyncable{sqlConfig, db}
}

func (s sqlSyncable) Sync(ctx context.Context, bytes []byte) error {
	return nil
}

func (s sqlSyncable) Close() error {
	return nil
}
