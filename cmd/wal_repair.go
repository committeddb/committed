package cmd

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

var (
	walRepairData   string
	walRepairCommit bool
	walRepairFrom   string
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

Run this only with the node STOPPED (enforced: a data directory whose node is
running is refused via its lock). A torn tail is a partial final record left
by a power loss mid-append; it was never acknowledged (raft never treated it as
committed), so dropping it is safe. A mid-log checksum failure is NOT a torn
tail — the tool refuses it and you should rebuild the node from a healthy
replica; see docs/operations/rebuild.md.

With --from <backup.tar[.gz]> (a backup of THIS node), a mid-log checksum
failure the backup covers is repaired from it: a corrupt record in a plain
segment is spliced byte-for-byte from the backup's copy; a corrupt compressed
segment is replaced by the backup's copy. Every splice is verified before it
is written (manifest hash, same log at the same alignment, raft-index
continuity, a clean re-scan) and refused otherwise, leaving the log untouched.
Run without --commit first to see the plan.

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

	if walRepairFrom != "" {
		return runWalSplice()
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

// runWalSplice is the --from path: repair mid-log corruption from a backup.
func runWalSplice() error {
	f, err := os.Open(walRepairFrom) //nolint:gosec // G304: the backup path is operator-supplied via --from
	if err != nil {
		return fmt.Errorf("open backup %q: %w", walRepairFrom, err)
	}
	defer func() { _ = f.Close() }()
	var r io.Reader = f
	if strings.HasSuffix(walRepairFrom, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("read gzip %q: %w", walRepairFrom, err)
		}
		defer func() { _ = gz.Close() }()
		r = gz
	}
	reports, err := wal.SpliceNode(walRepairData, r, walRepairCommit)
	if err != nil {
		return err
	}
	refused, planned, applied, remaining := 0, 0, 0, 0
	for _, rep := range reports {
		_, _ = fmt.Fprintf(os.Stdout, "%s: %s — %s\n", rep.Dir, rep.Before.Status, rep.Before.Detail)
		switch {
		case rep.Refused != "":
			refused++
			_, _ = fmt.Fprintf(os.Stdout, "  refused: %s\n", rep.Refused)
		case rep.Applied:
			applied++
			_, _ = fmt.Fprintf(os.Stdout, "  applied: %s\n  after: %s — %s\n", rep.Plan, rep.After.Status, rep.After.Detail)
			if rep.After.Status != wal.LogClean {
				remaining++
			}
		case rep.Plan != "":
			planned++
			_, _ = fmt.Fprintf(os.Stdout, "  would %s\n", rep.Plan)
		}
	}
	switch {
	case refused > 0:
		_, _ = fmt.Fprintln(os.Stdout, "\nsome corruption cannot be repaired from this backup: rebuild this node from a healthy replica; see docs/operations/rebuild.md")
		return fmt.Errorf("wal repair --from: %d log(s) refused", refused)
	case planned > 0 && !walRepairCommit:
		_, _ = fmt.Fprintln(os.Stdout, "\nsplice(s) planned; re-run with --commit to apply them")
	case applied > 0 && remaining > 0:
		_, _ = fmt.Fprintln(os.Stdout, "\nsplice(s) applied; a log still needs attention (a further corruption, or a torn tail — run wal repair without --from)")
		return fmt.Errorf("wal repair --from: %d log(s) still not clean after splicing", remaining)
	case applied > 0:
		_, _ = fmt.Fprintln(os.Stdout, "\nsplice(s) applied and verified; the node can be restarted")
	default:
		_, _ = fmt.Fprintln(os.Stdout, "\nnothing to splice")
	}
	return nil
}

var walDecompressData string

var walDecompressCmd = &cobra.Command{
	Use:   "decompress",
	Short: "Rewrite compressed WAL segments to the plain format for a downgrade",
	Long: `Rewrite a stopped node's compressed (.zst) event-log segments back to the
plain pre-0.8.0 segment format.

Run this only with the node STOPPED (enforced: a data directory whose node is
running is refused via its lock), and only when downgrading to a binary
older than 0.8.0 — those binaries do not recognize compressed segments and
would open a partial log. Upgrades need nothing: mixed logs read
transparently, and the background sealer re-compresses after the next start
on a 0.8.0+ binary.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if walDecompressData == "" {
			return fmt.Errorf("--data is required (the stopped node's data directory)")
		}
		counts, err := wal.DecompressNode(walDecompressData)
		if err != nil {
			return err
		}
		total := 0
		for dir, n := range counts {
			_, _ = fmt.Fprintf(os.Stdout, "%s: %d segment(s) rewritten\n", dir, n)
			total += n
		}
		if total == 0 {
			_, _ = fmt.Fprintln(os.Stdout, "no compressed segments found; the data dir is already downgrade-ready")
		} else {
			_, _ = fmt.Fprintln(os.Stdout, "done; the data dir is downgrade-ready")
		}
		return nil
	},
}

func init() {
	walRepairCmd.Flags().StringVar(&walRepairData, "data", "", "the stopped node's data directory; required")
	walRepairCmd.Flags().BoolVar(&walRepairCommit, "commit", false, "apply the repair (default: dry run, report only)")
	walRepairCmd.Flags().StringVar(&walRepairFrom, "from", "", "a backup (.tar or .tar.gz) of this node to splice mid-log corruption from")
	walDecompressCmd.Flags().StringVar(&walDecompressData, "data", "", "the stopped node's data directory; required")
	walCmd.AddCommand(walRepairCmd)
	walCmd.AddCommand(walDecompressCmd)
	rootCmd.AddCommand(walCmd)
}
