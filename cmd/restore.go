package cmd

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

var (
	restoreFrom       string
	restoreData       string
	restoreNodeID     uint64
	restoreNewCluster bool
	restoreClusterID  string
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a backup tar into a fresh data directory, as a member of a new cluster",
	Long: `Unpack a backup produced by "committed backup" into a new data directory,
as one machine of a new cluster. The manifest is validated and any archive
entry that would escape the target is rejected. The target --data directory
must not exist or must be empty; restore never overwrites existing data.

A restored cluster is a new cluster with its own cluster id: it never talks
to the cluster the backup came from, or to another restore of it. Restore
every machine from the same archive (the newest), each as a different member
of the backed-up cluster:

  committed restore --from ARCHIVE --data DIR --node-id 1 --new-cluster
      prints a cluster seed
  committed restore --from ARCHIVE --data DIR --node-id 2 --cluster-id SEED
  committed restore --from ARCHIVE --data DIR --node-id 3 --cluster-id SEED

Then start each node with the COMMITTED_NODE_ID it was restored as and a
COMMITTED_PEERS that names the new machines' addresses. Recorded addresses
are dropped; the restored node refuses to start under another node id.
Restored ingestables and syncables start automatically.

RTBF note: a backup is a frozen copy. Restoring resurrects any subject that
was deleted-and-scrubbed AFTER the backup was taken. committed keeps no ledger
of erased subjects (erasure is physical), so track right-to-be-forgotten
requests in your own compliance system and re-issue any that post-date the
backup after a restore. See docs/operations/backup.md.

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
	if restoreNodeID == 0 {
		return fmt.Errorf("--node-id is required: the member of the backed-up cluster this machine becomes")
	}
	seed, err := restoreSeed()
	if err != nil {
		return err
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

	var id cluster.ClusterID
	var members string
	m, err := backup.RestoreWith(r, restoreData, time.Now(), func(staging string, m *backup.Manifest) error {
		id = cluster.RestoredClusterID(seed, m.Fingerprint())
		lm, err := wal.RestoreAs(staging, id, restoreNodeID)
		if err != nil {
			return err
		}
		members = wal.MembersString(lm.ConfState)
		return nil
	})
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "restored %d files into %s (backup taken %s", len(m.Files), restoreData, m.CreatedAt)
	if m.NodeID != 0 {
		_, _ = fmt.Fprintf(os.Stdout, ", from node %d", m.NodeID)
	}
	_, _ = fmt.Fprintln(os.Stdout, ")")
	_, _ = fmt.Fprintf(os.Stdout, "node %d of a new cluster %s (members: %s)\n", restoreNodeID, id, members)
	if restoreNewCluster {
		_, _ = fmt.Fprintf(os.Stdout, "restore the other members from this same archive with --cluster-id %s\n", seed)
	}
	_, _ = fmt.Fprintf(os.Stdout, "start it with COMMITTED_NODE_ID=%d and COMMITTED_PEERS naming the new machines' addresses; see docs/operations/backup.md\n", restoreNodeID)
	return nil
}

// restoreSeed is the cluster seed: minted for --new-cluster, given by
// --cluster-id for the rest of the same restore.
func restoreSeed() (cluster.ClusterID, error) {
	switch {
	case restoreNewCluster && restoreClusterID != "":
		return cluster.ClusterID{}, fmt.Errorf("give --new-cluster for the first machine of a restore or --cluster-id for the rest, not both")
	case restoreNewCluster:
		return cluster.NewClusterID()
	case restoreClusterID != "":
		return cluster.ParseClusterID(restoreClusterID)
	}
	return cluster.ClusterID{}, fmt.Errorf("--new-cluster or --cluster-id is required: --new-cluster for the first machine of a restore (it prints a seed), --cluster-id SEED for the others")
}

func init() {
	restoreCmd.Flags().StringVar(&restoreFrom, "from", "", "backup path to restore (.tar or .tar.gz); required")
	restoreCmd.Flags().StringVar(&restoreData, "data", "", "empty target data directory; required")
	restoreCmd.Flags().Uint64Var(&restoreNodeID, "node-id", 0, "the member of the backed-up cluster this machine becomes; required")
	restoreCmd.Flags().BoolVar(&restoreNewCluster, "new-cluster", false, "first machine of a restore: mint a new cluster seed and print it")
	restoreCmd.Flags().StringVar(&restoreClusterID, "cluster-id", "", "the other machines of a restore: the seed --new-cluster printed")
	rootCmd.AddCommand(restoreCmd)
}
