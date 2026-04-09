package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/siddontang/go-log/log"
	"google.golang.org/protobuf/proto"
)

type MySQLDialect struct{}

func (m *MySQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	c, tables, err := createCanal(config.ConnectionString)
	if err != nil {
		return err
	}

	handler := &MySQLEventHandler{
		ctx:          ctx,
		config:       config,
		canal:        c,
		proposalChan: pr,
		positionChan: po,
		tables:       tables,
	}

	c.SetEventHandler(handler)

	if pos != nil {
		posProto := &dialectpb.MySQLBinLogPosition{}
		err := proto.Unmarshal(pos, posProto)
		if err != nil {
			return err
		}

		go c.RunFrom(mysql.Position{Name: posProto.Name, Pos: posProto.Pos})
	} else {
		go c.Run()
	}

	// Block until the worker cancels us, then ask canal to stop.
	//
	// IMPORTANT: c.Close() initiates shutdown but does NOT wait for
	// the goroutine started by `go c.Run()` above. (Reading the canal
	// source at v1.9.1: Close cancels canal's internal context and
	// closes the syncer/connection synchronously, then returns. There
	// is no WaitGroup join.) So when this function returns, the canal
	// goroutine may still be racing toward exit.
	//
	// We rely on two things to make that safe:
	//  1. Closing the syncer/connection causes the canal goroutine's
	//     binlog read to fail, so it exits promptly on its own.
	//  2. Our OnRow / OnPosSynced callbacks select on ctx.Done (which
	//     is already done by the time we get here) so if the canal
	//     goroutine fires a callback after Close, the callback bails
	//     out instead of blocking on a no-receiver channel send.
	//
	// The worker that called us (db.ingest) will return shortly after
	// we do, and its WaitGroup wait covers the inner Ingest goroutine
	// (this function) but not the canal goroutine. The canal goroutine
	// briefly outlives both, then exits when the read fails — leak
	// window is bounded by the binlog driver's read timeout.
	<-ctx.Done()
	c.Close()
	return nil
}

func (m *MySQLDialect) Close() error {
	return nil
}

type MySQLEventHandler struct {
	canal.DummyEventHandler
	// ctx is the Ingest caller's context. The OnRow / OnPosSynced
	// callbacks use it to abort their channel sends if the worker is
	// canceled mid-emit, otherwise the canal goroutine would leak
	// blocked on a no-receiver channel.
	//
	// Storing context.Context on a struct is an anti-pattern that
	// vet flags by default — Go's guidance is to pass ctx as the
	// first function argument so it's never long-lived state. We
	// accept the deviation here because the canal.EventHandler
	// interface (defined upstream in go-mysql-org/go-mysql) does not
	// take ctx in OnRow / OnPosSynced and we have no other way to
	// thread cancellation into those callbacks. The handler's lifetime
	// is exactly one Ingest call, so the stored ctx isn't long-lived
	// across calls and the usual "stale ctx" hazard doesn't apply.
	ctx          context.Context
	config       *sql.Config
	canal        *canal.Canal
	proposalChan chan<- *cluster.Proposal
	positionChan chan<- cluster.Position
	tables       []string
}

// TODO We will have to handle transactions at some point 1 transaction == 1 proposal
func (h *MySQLEventHandler) OnRow(e *canal.RowsEvent) error {
	if slices.Contains(h.tables, strings.ToLower(e.Table.Name)) {
		// switch e.Header.EventType {
		// case:
		// }

		m := make(map[string]any)
		row := e.Rows[len(e.Rows)-1]
		for i, c := range e.Table.Columns {
			val := row[i]
			// The MySQL binlog stream delivers TEXT/BLOB columns as []byte
			// while VARCHAR columns come through as string; the initial
			// mysqldump path that canal runs at startup delivers everything
			// as string. Coerce []byte to string here so the JSON we marshal
			// downstream is identical regardless of which path produced the
			// row. (json.Marshal of a []byte field base64-encodes it, which
			// would otherwise turn "one" into "b25l".) Binary BLOB columns
			// are not a concern for any current caller of this dialect.
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			m[strings.ToLower(c.Name)] = val
		}

		toJSON := make(map[string]any)
		for _, mapping := range h.config.Mappings {
			value := m[mapping.SQLColumn]
			toJSON[mapping.JsonName] = value
		}

		jsonString, err := json.Marshal(toJSON)
		if err != nil {
			return err
		}

		primaryKey := h.config.PrimaryKey
		key := fmt.Sprintf("%v", m[primaryKey])

		// Stamp the propose-time wall-clock so apply on every node
		// records the same time-series timestamp. Without this, the
		// time-series store sees a different value per node and a
		// post-snapshot replay would write a fresh "now" instead of
		// the original. See cluster.Entity.Timestamp.
		p := &cluster.Proposal{
			Entities: []*cluster.Entity{{
				Type:      h.config.Type,
				Key:       []byte(key),
				Data:      []byte(jsonString),
				Timestamp: time.Now().UnixMilli(),
			}},
		}

		select {
		case h.proposalChan <- p:
		case <-h.ctx.Done():
			return h.ctx.Err()
		}

		fmt.Printf("[OnRow] [%v] Action: [%s] Rows:[%v]\n", e.Table, e.Action, e.Rows)
	}

	return nil
}

func (h *MySQLEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// An XID event is generated for a COMMIT of a transaction that modifies one or more tables of an XA-capable storage engine.
	fmt.Printf("header: %v\n", header)
	if header != nil && header.EventType == replication.XID_EVENT {
		pos := &dialectpb.MySQLBinLogPosition{Name: pos.Name, Pos: pos.Pos}
		bs, err := proto.Marshal(pos)
		if err != nil {
			return err
		}

		select {
		case h.positionChan <- bs:
		case <-h.ctx.Done():
			return h.ctx.Err()
		}
		fmt.Printf("[OnPosSynced] Header: [%v] pos:[%v]\n", header, pos)
	}

	return nil
}

func (h *MySQLEventHandler) String() string {
	return "MyEventHandler"
}

func createCanal(connectionString string) (*canal.Canal, []string, error) {
	u, err := url.Parse(connectionString)
	if err != nil {
		return nil, nil, err
	}

	addr := u.Host
	username := u.User.Username()
	password, passwordExists := u.User.Password()
	database := strings.TrimPrefix(u.Path, "/")
	tables := strings.Split(u.Query().Get("tables"), ",")

	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = username
	if passwordExists {
		cfg.Password = password
	}
	cfg.Dump.TableDB = database
	cfg.Dump.Tables = tables
	streamHandler, err := log.NewStreamHandler(os.Stdout)
	if err != nil {
		return nil, nil, err
	}
	logger := log.NewDefault(streamHandler)
	logger.SetLevel(log.LevelError)
	cfg.Logger = logger

	canal, err := canal.NewCanal(cfg)

	return canal, tables, err
}
