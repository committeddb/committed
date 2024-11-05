package cmd

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed demo/migrations/*.sql
var embedMigrations embed.FS

var demoSeederCmd = &cobra.Command{
	Use:   "demo-seeder",
	Short: "Seeds the demo databases",
	Long:  `Seeds the demo databases`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Seeding...")

		db, err := sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/")
		if err != nil {
			panic(err)
		}

		_, err = db.Exec("CREATE DATABASE IF NOT EXISTS source;")
		if err != nil {
			panic(err)
		}
		db.Close()

		db, err = sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/source")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		goose.SetBaseFS(embedMigrations)

		if err := goose.SetDialect("mysql"); err != nil {
			panic(err)
		}

		if err := goose.Up(db, "demo/migrations"); err != nil {
			panic(err)
		}

		db.Exec(`INSERT INTO simple (title,body) VALUES ('title-one', 'body-one');`)
		db.Exec(`INSERT INTO simple (title,body) VALUES ('title-two', 'body-two');`)
	},
}

func init() {
	rootCmd.AddCommand(demoSeederCmd)
}
