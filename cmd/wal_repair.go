package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

var (
	walRepairData   string
	walRepairCommit bool
)

var walCmd = &cobra.Command{
	Use:   "wal",
	Short: "Offline maintenance on a stopped node's write-ahead logs",
}

var walRepairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Diagnose and repair a torn WAL tail on a stopped node",
	Long: `Scan a stopped node's write-ahead logs and, with --commit, truncate a torn
trailing record so the log opens again.

Run this only with the node STOPPED. A torn tail is a partial final record left
by a power loss mid-append; it was never acknowledged (raft never treated it as
committed), so dropping it is safe. A mid-log checksum failure is NOT a torn
tail — the tool refuses it and you should rebuild the node from a healthy
replica; see docs/operations/rebuild.md.

Defaults to a dry run: it reports what it finds and changes nothing until you
pass --commit.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWalRepair()
	},
}

func runWalRepair() error {
	if walRepairData == "" {
		return fmt.Errorf("--data is required (the stopped node's data directory)")
	}

	results, err := wal.RepairNode(walRepairData, walRepairCommit)
	if err != nil {
		return err
	}

	corrupt, torn := false, false
	for _, d := range results {
		_, _ = fmt.Fprintf(os.Stdout, "%s: %s — %s\n", d.Dir, d.Status, d.Detail)
		switch d.Status {
		case wal.LogCorrupt:
			corrupt = true
		case wal.LogTornTail:
			torn = true
		}
	}

	switch {
	case corrupt:
		_, _ = fmt.Fprintln(os.Stdout, "\nnon-recoverable corruption (checksum failure or mid-compaction): rebuild this node from a healthy replica; see docs/operations/rebuild.md")
		return fmt.Errorf("wal repair: corruption that is not a torn tail; rebuild required")
	case torn && !walRepairCommit:
		_, _ = fmt.Fprintln(os.Stdout, "\ntorn tail(s) found; re-run with --commit to truncate the unacknowledged trailing record(s)")
	case torn && walRepairCommit:
		_, _ = fmt.Fprintln(os.Stdout, "\ntorn tail(s) truncated; the node can be restarted")
	default:
		_, _ = fmt.Fprintln(os.Stdout, "\nall logs are clean")
	}
	return nil
}

func init() {
	walRepairCmd.Flags().StringVar(&walRepairData, "data", "", "the stopped node's data directory; required")
	walRepairCmd.Flags().BoolVar(&walRepairCommit, "commit", false, "truncate a torn tail (default: dry run, report only)")
	walCmd.AddCommand(walRepairCmd)
	rootCmd.AddCommand(walCmd)
}
