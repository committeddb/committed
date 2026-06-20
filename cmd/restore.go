package cmd

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/cluster/backup"
)

var (
	restoreFrom string
	restoreData string
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a backup tar into a fresh data directory",
	Long: `Unpack a backup produced by "committed backup" into a new data directory.
The manifest is validated and any archive entry that would escape the target
is rejected. The target --data directory must not exist or must be empty;
restore never overwrites existing data.

After restoring, start a node against the directory with the SAME
COMMITTED_NODE_ID and COMMITTED_PEERS the source node used — the restored
directory IS a node's directory, so it recovers exactly as it would from its
own disk.

RTBF note: a backup is a frozen copy. Restoring resurrects any subject that
was deleted-and-scrubbed AFTER the backup was taken. To stay compliant after a
restore, replay the right-to-be-forgotten deletes recorded in the audit log
that post-date the backup. See docs/operations/backup.md.

If --from ends in ".gz" it is read as gzip.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRestore()
	},
}

func runRestore() error {
	if restoreFrom == "" {
		return fmt.Errorf("--from is required (the backup .tar or .tar.gz path)")
	}
	if restoreData == "" {
		return fmt.Errorf("--data is required (the empty target data directory)")
	}

	f, err := os.Open(restoreFrom) //nolint:gosec // G304: the backup path is operator-supplied via --from
	if err != nil {
		return fmt.Errorf("open backup %q: %w", restoreFrom, err)
	}
	defer func() { _ = f.Close() }()

	var r io.Reader = f
	if strings.HasSuffix(restoreFrom, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("read gzip %q: %w", restoreFrom, err)
		}
		defer func() { _ = gz.Close() }()
		r = gz
	}

	m, err := backup.Restore(r, restoreData, time.Now())
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "restored %d files into %s (backup taken %s", len(m.Files), restoreData, m.CreatedAt)
	if m.NodeID != 0 {
		_, _ = fmt.Fprintf(os.Stdout, ", node %d", m.NodeID)
	}
	_, _ = fmt.Fprintln(os.Stdout, ")")
	_, _ = fmt.Fprintln(os.Stdout, "start a node against this directory with the source node's COMMITTED_NODE_ID and COMMITTED_PEERS; see docs/operations/backup.md")
	return nil
}

func init() {
	restoreCmd.Flags().StringVar(&restoreFrom, "from", "", "backup path to restore (.tar or .tar.gz); required")
	restoreCmd.Flags().StringVar(&restoreData, "data", "", "empty target data directory; required")
	rootCmd.AddCommand(restoreCmd)
}
