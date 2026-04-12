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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type MySQLDialect struct{}

// canalBackoff{Min,Max} bound the retry interval for createCanal.
// A transient MySQL outage (DNS blip, container restart, network
// partition) at Ingest startup used to immediately return an error,
// leaving the worker leader-but-not-ingesting with no recovery path.
// The retry loop below caps at Max and is bounded by ctx so a
// shutdown still propagates promptly.
const (
	canalBackoffMin = 1 * time.Second
	canalBackoffMax = 30 * time.Second

	// maxPendingEntities is the soft limit on buffered entities per
	// transaction. If a single MySQL transaction modifies more than
	// this many rows, the handler emits a partial proposal to avoid
	// unbounded memory growth. This breaks atomicity for oversized
	// transactions — an acceptable trade-off versus OOM-ing the process.
	maxPendingEntities = 10000
)

func (m *MySQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	backoff := canalBackoffMin

	// Outer loop: each iteration creates a canal, runs it until it
	// exits (connection loss, OnRow error, etc.), then reconnects with
	// backoff. Only ctx cancellation breaks out.
	for {
		// --- create canal with retry ---
		var c *canal.Canal
		var tables []string
		for {
			var err error
			c, tables, err = createCanal(config.ConnectionString)
			if err == nil {
				backoff = canalBackoffMin
				break
			}

			zap.L().Warn("createCanal failed, retrying",
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > canalBackoffMax {
				backoff = canalBackoffMax
			}
		}

		// --- set up handler and start the canal goroutine ---
		handler := &MySQLEventHandler{
			ctx:          ctx,
			config:       config,
			canal:        c,
			proposalChan: pr,
			positionChan: po,
			tables:       tables,
		}

		c.SetEventHandler(handler)

		// Determine the resume position: prefer the handler's last
		// emitted position (from a previous iteration) over the
		// original pos passed into Ingest.
		var resumePos *mysql.Position
		if handler.lastPos != nil {
			resumePos = handler.lastPos
		} else if pos != nil {
			posProto := &dialectpb.MySQLBinLogPosition{}
			if err := proto.Unmarshal(pos, posProto); err != nil {
				c.Close()
				return err
			}
			resumePos = &mysql.Position{Name: posProto.Name, Pos: posProto.Pos}
		}

		// Capture the Run/RunFrom return so we know when the canal
		// goroutine exits — a non-nil error means the stream broke
		// (connection lost, OnRow error propagated, etc.) and we
		// should reconnect.
		canalDone := make(chan error, 1)
		if resumePos != nil {
			go func() { canalDone <- c.RunFrom(*resumePos) }()
		} else {
			go func() { canalDone <- c.Run() }()
		}

		// --- wait for canal exit or context cancel ---
		select {
		case <-ctx.Done():
			c.Close()
			return nil
		case err := <-canalDone:
			// Canal exited on its own — connection lost, fatal
			// binlog error, or an OnRow error that was returned to
			// canal. Close the old canal and reconnect.
			zap.L().Warn("canal exited, will reconnect",
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)
			c.Close()
		}

		// --- backoff before reconnect ---
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > canalBackoffMax {
			backoff = canalBackoffMax
		}
	}
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

	// lastPos holds the most recently emitted binlog position so the
	// outer reconnect loop can resume from where it left off instead
	// of re-dumping from scratch or replaying from the original pos.
	lastPos *mysql.Position

	// pending accumulates entities from OnRow calls until the
	// transaction commits (OnXID). This ensures one MySQL transaction
	// maps to one cluster.Proposal, giving consumers atomic delivery
	// and enabling exactly-once semantics via post-commit position
	// checkpointing.
	pending []*cluster.Entity
}

func (h *MySQLEventHandler) OnRow(e *canal.RowsEvent) error {
	if !slices.Contains(h.tables, strings.ToLower(e.Table.Name)) {
		return nil
	}

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
		zap.L().Warn("OnRow: skipping row with unmarshalable data",
			zap.String("table", e.Table.Name),
			zap.Error(err),
		)
		return nil
	}

	primaryKey := h.config.PrimaryKey
	key := fmt.Sprintf("%v", m[primaryKey])

	// Buffer the entity until the transaction commits (OnXID). Stamp
	// the propose-time wall-clock so apply on every node records the
	// same time-series timestamp. See cluster.Entity.Timestamp.
	h.pending = append(h.pending, &cluster.Entity{
		Type:      h.config.Type,
		Key:       []byte(key),
		Data:      []byte(jsonString),
		Timestamp: time.Now().UnixMilli(),
	})

	// Soft limit: emit a partial batch to prevent OOM on very large
	// transactions. This breaks atomicity for oversized transactions
	// but is the right trade-off versus unbounded memory growth.
	if len(h.pending) >= maxPendingEntities {
		if err := h.flushPending(); err != nil {
			return err
		}
	}

	zap.L().Debug("OnRow", zap.String("table", e.Table.Name), zap.String("action", e.Action))
	return nil
}

// flushPending emits all buffered entities as a single proposal and
// resets the buffer. No-op when the buffer is empty.
func (h *MySQLEventHandler) flushPending() error {
	if len(h.pending) == 0 {
		return nil
	}
	p := &cluster.Proposal{Entities: h.pending}
	select {
	case h.proposalChan <- p:
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
	h.pending = nil
	return nil
}

// OnXID is called when a MySQL transaction commits. It flushes all
// buffered entities as a single proposal and checkpoints the post-commit
// binlog position. This ensures one MySQL transaction maps to one
// cluster.Proposal and that the checkpoint position is always past a
// fully committed transaction — no duplicates or gaps on restart.
func (h *MySQLEventHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	if err := h.flushPending(); err != nil {
		return err
	}

	posProto := &dialectpb.MySQLBinLogPosition{Name: pos.Name, Pos: pos.Pos}
	bs, err := proto.Marshal(posProto)
	if err != nil {
		return err
	}

	select {
	case h.positionChan <- bs:
	case <-h.ctx.Done():
		return h.ctx.Err()
	}

	h.lastPos = &mysql.Position{Name: pos.Name, Pos: pos.Pos}

	zap.L().Debug("OnXID", zap.String("pos", pos.Name), zap.Uint32("offset", pos.Pos))
	return nil
}

func (h *MySQLEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// Flush any entities accumulated during the dump phase. During
	// binlog replication OnXID already flushed before this is called,
	// so pending will be empty here.
	return h.flushPending()
}

func (h *MySQLEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if queryEvent == nil {
		zap.L().Warn("OnDDL: DDL event with nil query",
			zap.String("pos", nextPos.Name),
			zap.Uint32("offset", nextPos.Pos),
		)
		return nil
	}
	zap.L().Warn("OnDDL: DDL event received",
		zap.String("schema", string(queryEvent.Schema)),
		zap.String("query", string(queryEvent.Query)),
		zap.String("pos", nextPos.Name),
		zap.Uint32("offset", nextPos.Pos),
	)
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
