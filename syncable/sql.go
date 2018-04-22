package syncable

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/cznic/ql"
	// The driver will be loaded through reflection
	_ "github.com/lib/pq"

	"github.com/oliveagle/jsonpath"
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
	config  sqlConfig
	inserts []sqlInsert
	db      *sql.DB
}

type sqlInsert struct {
	stmt     *sql.Stmt
	jsonPath []string
}

// NewSQLSyncable creates a new syncable
func newSQLSyncable(sqlConfig sqlConfig) *sqlSyncable {
	if sqlConfig.driver == "ql" {
		ql.RegisterDriver()
	}

	db, err := sql.Open(sqlConfig.driver, sqlConfig.connectionString)
	if err != nil {
		log.Fatal(err)
	}

	inserts := unwrapMappings(db, sqlConfig.mappings)

	return &sqlSyncable{sqlConfig, inserts, db}
}

func unwrapMappings(db *sql.DB, mappings []sqlMapping) []sqlInsert {
	var tables = make(map[string][]sqlMapping)

	for _, item := range mappings {
		if tables[item.table] == nil {
			tables[item.table] = []sqlMapping{item}
		} else {
			tables[item.table] = append(tables[item.table], item)
		}
	}

	var sqlInserts []sqlInsert
	for table, sqlMappings := range tables {
		var jsonPaths []string
		for _, item := range sqlMappings {
			jsonPaths = append(jsonPaths, item.jsonPath)
		}

		sql := createSQL(table, sqlMappings)

		stmt, err := db.Prepare(sql)
		if err != nil {
			log.Fatalf("Error Preparing sql [%s]: %v", sql, err)
		}
		sqlInserts = append(sqlInserts, sqlInsert{stmt, jsonPaths})
	}

	return sqlInserts
}

func createSQL(table string, sqlMappings []sqlMapping) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", table)
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "$%d", i+1)
		} else {
			fmt.Fprintf(&sql, ",$%d", i+1)
		}
	}
	fmt.Fprint(&sql, ")")

	return sql.String()
}

func (s sqlSyncable) Sync(ctx context.Context, bytes []byte) error {
	var jsonData interface{}
	json.Marshal(string(bytes))
	err := json.Unmarshal(bytes, &jsonData)
	if err != nil {
		log.Printf("Error Unmarshalling json: %v", err)
		return err
	}

	for _, insert := range s.inserts {
		var values []interface{}

		for _, path := range insert.jsonPath {
			res, err := jsonpath.JsonPathLookup(jsonData, path)
			if err != nil {
				log.Printf("Error while parsing [%v] in [%v]: %v\n", path, jsonData, err)
				res = ""
			}
			values = append(values, res)
		}

		tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: 0, ReadOnly: false})
		defer tx.Commit()
		if err != nil {
			log.Printf("Error while creating transaction: %v", err)
			return err
		}
		_, err = tx.Stmt(insert.stmt).ExecContext(ctx, values...)
		if err != nil {
			log.Printf("Error while executing statement: %v", err)
			return err
		}
	}

	return nil
}

func (s sqlSyncable) Topics() []string {
	return []string{s.config.topic}
}

func (s sqlSyncable) Close() error {
	return s.db.Close()
}
