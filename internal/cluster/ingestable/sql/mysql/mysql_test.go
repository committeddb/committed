package mysql_test

import (
	gosql "database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
)

var port = "3306"

const dbName = "dbName"
const username = "root"
const password = "secret"

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err := pool.Run("mysql", "9", []string{fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", password)})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	port = resource.GetPort("3306/tcp")

	if err := pool.Retry(func() error {
		var err error
		db, err := gosql.Open("mysql", fmt.Sprintf("%s:%s@(localhost:%s)/", username, password, port))
		if err != nil {
			return err
		}
		err = db.Ping()
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE DATABASE " + dbName)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	if err := resource.Expire(30); err != nil { // Tell docker to hard kill the container in 30 seconds
		log.Fatalf("Could not expire resource: %s", err)
	}

	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	m.Run()
}

func TestMysqlDialect(t *testing.T) {
	simpleType := &cluster.Type{
		ID:   "simple",
		Name: "simple",
	}
	basicConfig := &sql.Config{
		Type: simpleType,
		Mappings: []sql.Mapping{{
			JsonName:  "one",
			SQLColumn: "one",
		}, {
			JsonName:  "pk",
			SQLColumn: "pk",
		}},
		PrimaryKey: "pk",
	}

	p1 := &cluster.Proposal{
		Entities: []*cluster.Entity{{
			Type: simpleType,
			Key:  []byte("key1"),
			Data: []byte("{\"one\":\"one\",\"pk\":\"key1\"}"),
		}},
	}
	p2 := &cluster.Proposal{
		Entities: []*cluster.Entity{{
			Type: simpleType,
			Key:  []byte("key2"),
			Data: []byte("{\"one\":\"two\",\"pk\":\"key2\"}"),
		}},
	}

	tests := []struct {
		name    string
		config  *sql.Config
		tables  string
		setupFn func(*testing.T)
		ps      []*cluster.Proposal
	}{
		{"one-simple", basicConfig, "table", setup1, []*cluster.Proposal{p1}},
		{"two-simple", basicConfig, "table", setup2, []*cluster.Proposal{p1, p2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect := &mysql.MySQLDialect{}
			con := fmt.Sprintf("mysql://%s:%s@127.0.0.1:%s/%s?tables=%s", username, password, port, dbName, tt.tables)
			tt.config.ConnectionString = con
			proposalChan, positionChan, closer, err := dialect.Open(tt.config, nil)
			require.Nil(t, err)
			defer closer.Close()

			tt.setupFn(t)

			expectedCount := 2

			var proposals []*cluster.Proposal
			var positions []*cluster.Position
		outer:
			for {
				select {
				case proposal := <-proposalChan:
					proposals = append(proposals, proposal)
				case position := <-positionChan:
					positions = append(positions, &position)
				default:
					if len(proposals)+len(positions) >= expectedCount {
						break outer
					}
				}
			}

			require.ElementsMatch(t, tt.ps, proposals)
		})
	}
}

func setup1(t *testing.T) {
	table := "table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL,one TEXT,PRIMARY KEY (pk));", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, one) VALUES ('key1', 'one');", table))
	require.Nil(t, err)
}

func setup2(t *testing.T) {
	table := "table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL,one TEXT,PRIMARY KEY (pk));", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, one) VALUES ('key1', 'one');", table))
	require.Nil(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, one) VALUES ('key2', 'two');", table))
	require.Nil(t, err)
}

func createDB(t *testing.T) *gosql.DB {
	db, err := gosql.Open("mysql", fmt.Sprintf("%s:%s@(localhost:%s)/%s", username, password, port, dbName))
	require.Nil(t, err)
	err = db.Ping()
	require.Nil(t, err)

	return db
}
