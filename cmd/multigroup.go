package cmd

import (
	"fmt"

	"github.com/philborlin/committed/internal/controlplane/raft"
	"github.com/spf13/cobra"
)

var multigroupCmd = &cobra.Command{
	Use:   "multigroup",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		f := cmd.Flags()

		err := f.Parse(args)
		if err != nil {
			panic(err)
		}
		replicaID, err := f.GetInt("replicaid")
		if err != nil {
			panic(err)
		}

		fmt.Println("multigroup called")
		raft.Raft(replicaID)
	},
}

func init() {
	rootCmd.AddCommand(multigroupCmd)

	multigroupCmd.Flags().Int("replicaid", 1, "The replicaID for the node")
}
