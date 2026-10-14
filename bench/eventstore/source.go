package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
)

// source is the events table in one engine.
type source struct {
	engine string
	db     *gosql.DB
	insert string
}

func openSource(o options, ddl string) (*source, error) {
	driver := map[string]string{"postgres": "pgx", "mysql": "mysql", "sqlserver": "sqlserver"}[o.engine]
	db, err := gosql.Open(driver, o.srcDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(o.writers + 4)
	db.SetMaxIdleConns(o.writers + 4)
	s := &source{engine: o.engine, db: db}
	table, ph := "events", func(i int) string { return fmt.Sprintf("$%d", i) }
	switch o.engine {
	case "mysql":
		ph = func(int) string { return "?" }
		ddl = fmt.Sprintf(`CREATE TABLE events (
  seq_id    BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stream_id CHAR(36)     NOT NULL,
  version   INT          NOT NULL,
  type      VARCHAR(64)  NOT NULL,
  data      %s           NOT NULL,
  timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE (stream_id, version))`, o.payload)
	case "sqlserver":
		table = "dbo.events"
		ph = func(i int) string { return fmt.Sprintf("@p%d", i) }
		ddl = fmt.Sprintf(`CREATE TABLE dbo.events (
  seq_id      BIGINT           NOT NULL IDENTITY PRIMARY KEY,
  stream_id   UNIQUEIDENTIFIER NOT NULL,
  version     INT              NOT NULL,
  type        NVARCHAR(64)     NOT NULL,
  data        %s               NOT NULL,
  [timestamp] DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
  UNIQUE (stream_id, version))`, o.payload)
	default:
		// The recipe's own CREATE TABLE, with the payload type swapped if asked.
		if !strings.EqualFold(o.payload, "jsonb") {
			ddl = strings.Replace(ddl, "jsonb", o.payload, 1)
		}
	}
	s.insert = fmt.Sprintf("INSERT INTO %s (stream_id, version, type, data) VALUES (%s, %s, %s, %s)",
		table, ph(1), ph(2), ph(3), ph(4))
	if _, err := db.ExecContext(context.Background(), ddl); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create the events table: %w", err)
	}
	return s, nil
}

// append inserts one event in its own transaction (autocommit).
func (s *source) append(ctx context.Context, e event) error {
	_, err := s.db.ExecContext(ctx, s.insert, e.stream, e.version, e.typ, e.data)
	return err
}

func (s *source) close() { _ = s.db.Close() }
