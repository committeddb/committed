package cmd

import (
	"flag"
	"fmt"
	"log"

	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/philborlin/committed/internal/cluster/http"
	ingestablesql "github.com/philborlin/committed/internal/cluster/ingestable/sql"
	ingestablemysql "github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	syncsql "github.com/philborlin/committed/internal/cluster/syncable/sql"
	syncmysql "github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
	"github.com/spf13/cobra"
)

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("node called")

		url := flag.String("url", "http://127.0.0.1:9022", "url with port")
		id := flag.Uint64("id", 1, "node ID")
		addr := flag.String("addr", ":8080", "node ID")
		var sync chan *db.SyncableWithID
		var ingest chan *db.IngestableWithID

		p := parser.New()
		s, err := wal.Open("./data", p, sync, ingest)
		if err != nil {
			log.Fatalf("cannot open storage: %v", err)
		}

		peers := make(db.Peers)
		peers[*id] = *url

		db := db.New(*id, peers, s, p, sync, ingest)
		fmt.Printf("Raft Running...\n")
		h := http.New(db)
		fmt.Printf("API Listening on %s...\n", *addr)

		db.AddDatabaseParser("sql", dbParser())
		db.AddIngestableParser("sql", ingestableParser(db))
		db.AddSyncableParser("sql", &syncsql.SyncableParser{})

		db.EatCommitC()

		go func() {
			if err := h.ListenAndServe(*addr); err != nil {
				log.Fatal(err)
			}
		}()

		if err, ok := <-db.ErrorC; ok {
			log.Fatalf("raft error: %v", err)
		}
	},
}

func dbParser() *syncsql.DBParser {
	ds := make(map[string]syncsql.Dialect)
	p := &syncsql.DBParser{Dialects: ds}
	ds["mysql"] = &syncmysql.MySQLDialect{}
	return p
}

func ingestableParser(t ingestablesql.Typer) *ingestablesql.IngestableParser {
	p := ingestablesql.NewIngestableParser(t)
	p.Dialects["mysql"] = &ingestablemysql.MySQLDialect{}
	return p
}

func init() {
	rootCmd.AddCommand(nodeCmd)
}
