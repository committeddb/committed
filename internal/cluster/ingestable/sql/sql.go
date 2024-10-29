package sql

import (
	"context"
	"database/sql"

	"github.com/philborlin/committed/internal/cluster"
)

type Read struct {
}

type Ingestable struct {
	db      *sql.DB
	config  *Config
	dialect Dialect
	insert  *Read
}

func New(d *DB, config *Config) *Ingestable {
	return &Ingestable{db: d.DB, config: config, dialect: d.dialect}
}

func (i *Ingestable) Ingest(ctx context.Context) (cluster.ShouldSnapshot, *cluster.Proposal, error) {
	// cfg := replication.BinlogSyncerConfig{
	// 	ServerID: 100,
	// 	Flavor:   "mysql",
	// 	Host:     "127.0.0.1",
	// 	Port:     3306,
	// 	User:     "root",
	// 	Password: "",
	// }
	// syncer := replication.NewBinlogSyncer(cfg)

	// syncer.

	// streamer, _ := syncer.StartSync(mysql.Position{binlogFile, binlogPos})

	return false, nil, nil
}

func (i *Ingestable) Init() error {
	return nil
}

func (i *Ingestable) Close() error {
	return nil
}
