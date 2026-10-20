package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

var (
	recoverData   string
	recoverNodeID uint64
)

var recoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Make one stopped survivor of a cluster that lost its majority the only member of a new cluster",
	Long: `Bring a cluster back after losing a majority of its voters. Run it on ONE
stopped survivor — the one whose log reaches furthest (the highest last
index) — and it becomes the only member of a new cluster:

  - every other member is removed (a joint configuration is left first);
  - its whole log is kept and committed, including entries the old cluster
    never committed;
  - it gets a new cluster id, so the old members can never talk to it again.

Then start it, and add the other machines back under NEW node ids with empty
data directories (see docs/operations/membership.md). The old majority must
never run again: wipe the other survivors before re-adding them.

  COMMITTED_DATA_DIR   default for --data (falls back to ./data)
  COMMITTED_NODE_ID    default for --node-id`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRecover()
	},
}

func runRecover() error {
	dataDir := recoverData
	if dataDir == "" {
		dataDir = getenvDefault("COMMITTED_DATA_DIR", "./data")
	}
	nodeID := recoverNodeID
	if nodeID == 0 {
		raw := strings.TrimSpace(os.Getenv("COMMITTED_NODE_ID"))
		if raw == "" {
			return fmt.Errorf("--node-id is required (or COMMITTED_NODE_ID): this survivor's node id")
		}
		v, err := parseNodeID(raw)
		if err != nil {
			return err
		}
		nodeID = v
	}

	r, err := wal.Recover(dataDir, nodeID)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	if c := r.Converted; c != nil {
		_, _ = fmt.Fprintf(os.Stdout, "converted its tidwall event log to the segmented engine: %d records through raft index %d\n", c.Records, c.LastIndex)
	}
	removed := "none"
	if len(r.Removed) > 0 {
		ids := make([]string, len(r.Removed))
		for i, id := range r.Removed {
			ids[i] = fmt.Sprint(id)
		}
		removed = strings.Join(ids, ", ")
	}
	_, _ = fmt.Fprintf(os.Stdout, "node %d is now the only member of cluster %s; its log is committed through index %d\n", nodeID, r.ClusterID, r.LastIndex)
	_, _ = fmt.Fprintf(os.Stdout, "removed members: %s — they must never run again with their old data; wipe them and add the machines back under new node ids\n", removed)
	_, _ = fmt.Fprintf(os.Stdout, "start it with COMMITTED_NODE_ID=%d and COMMITTED_PEERS naming only itself\n", nodeID)
	return nil
}

func init() {
	recoverCmd.Flags().StringVar(&recoverData, "data", "", "the survivor's data directory (default $COMMITTED_DATA_DIR or ./data)")
	recoverCmd.Flags().Uint64Var(&recoverNodeID, "node-id", 0, "the survivor's node id (default $COMMITTED_NODE_ID)")
	rootCmd.AddCommand(recoverCmd)
}
