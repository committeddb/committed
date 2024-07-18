package cmd

import (
	"github.com/spf13/cobra"

	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/philborlin/committed/internal/node/api"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/node/cluster"
	"github.com/philborlin/committed/internal/node/raft"

	// Ensures that init() functions run
	_ "github.com/philborlin/committed/internal/node/syncable"
	_ "github.com/philborlin/committed/internal/node/syncable/file"
	_ "github.com/philborlin/committed/internal/node/syncable/sql"
)

var legacyCmd = &cobra.Command{
	Use:   "legacy",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		nodeURLs := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
		id := flag.Int("id", 1, "node ID")
		apiPort := flag.Int("port", 9121, "API server port")
		join := flag.Bool("join", false, "join an existing cluster")
		flag.Parse()

		proposeC := make(chan []byte)
		defer close(proposeC)
		confChangeC := make(chan raftpb.ConfChange)
		defer close(confChangeC)

		var c *cluster.Cluster
		getSnapshot := func() ([]byte, error) { return c.GetSnapshot() }

		nodes := strings.Split(*nodeURLs, ",")
		baseDir := "data"
		if _, err := os.Stat(baseDir); err != nil {
			if err := os.MkdirAll(baseDir, 0750); err != nil {
				log.Fatal("cannot create data dir")
			}
		}
		commitC, errorC, snapshotterReady, leader := raft.NewRaftNode(
			*id, nodes, *join, baseDir, getSnapshot, proposeC, confChangeC)

		c = cluster.New(<-snapshotterReady, proposeC, commitC, errorC, baseDir, leader, *id)

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			<-sigs
			log.Println("Received a signal to shutdown. Shutting down...")
			err := c.Shutdown()
			if err != nil {
				log.Println(err)
			}
			log.Println("Graceful shutdown complete. Exiting...")
			os.Exit(0)
		}()

		api.ServeAPI(c, *apiPort, errorC)
	},
}

func init() {
	rootCmd.AddCommand(legacyCmd)
}
