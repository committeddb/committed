package mysql

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"strings"

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

func (m *MySQLDialect) Open(config *sql.Config, pos cluster.Position) (<-chan *cluster.Proposal, <-chan cluster.Position, io.Closer, error) {
	c, tables, err := createCanal(config.ConnectionString)
	if err != nil {
		return nil, nil, nil, err
	}

	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)
	handler := &MySQLEventHandler{
		config:       config,
		canal:        c,
		proposalChan: proposalChan,
		positionChan: positionChan,
		tables:       tables,
	}

	c.SetEventHandler(handler)

	if pos != nil {
		posProto := &dialectpb.MySQLBinLogPosition{}
		err := proto.Unmarshal(pos, posProto)
		if err != nil {
			return nil, nil, nil, err
		}

		go c.RunFrom(mysql.Position{Name: posProto.Name, Pos: posProto.Pos})
	} else {
		go c.Run()
	}

	return proposalChan, positionChan, handler, nil
}

type MySQLEventHandler struct {
	canal.DummyEventHandler
	config       *sql.Config
	canal        *canal.Canal
	proposalChan chan<- *cluster.Proposal
	positionChan chan<- cluster.Position
	tables       []string
}

func (h *MySQLEventHandler) Close() error {
	h.canal.Close()
	return nil
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
			m[strings.ToLower(c.Name)] = row[i]
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

		p := &cluster.Proposal{
			Entities: []*cluster.Entity{{
				Type: h.config.Type,
				Key:  []byte(key),
				Data: []byte(jsonString),
			}},
		}

		h.proposalChan <- p

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

		h.positionChan <- bs
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
