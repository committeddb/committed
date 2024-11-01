package cmd

import (
	"fmt"
	"io"
	"log"

	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/spf13/cobra"
)

var controlplaneCmd = &cobra.Command{
	Use:   "proposals",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		s, err := wal.Open("./data", nil, nil, nil)
		if err != nil {
			log.Fatalf("cannot open storage: %v", err)
		}

		r := s.Reader("")
		for {
			_, p, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("reader error: %v", err)
			}

			fmt.Println(p)
		}
	},
}

func init() {
	rootCmd.AddCommand(controlplaneCmd)
}
