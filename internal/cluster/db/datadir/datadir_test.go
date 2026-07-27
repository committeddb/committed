package datadir_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

func writeFileIn(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

// TestCanonicalArchiveEntry pins the canonical-vs-residue taxonomy that both the
// backup selector and (as the mutating twin) the Open sweep depend on.
func TestCanonicalArchiveEntry(t *testing.T) {
	tests := []struct {
		name          string
		rel           string
		eventsPresent bool
		wantKeep      bool
		wantRel       string
	}{
		{"canonical event log", "events/0001", true, true, "events/0001"},
		{"canonical entry log", "raft/log/0001", true, true, "raft/log/0001"},
		{"canonical state log", "raft/state/0001", true, true, "raft/state/0001"},
		{"canonical bbolt", "metadata/bbolt.db", true, true, "metadata/bbolt.db"},
		{"retired skipped when events present", "events.retired/0001", true, false, ""},
		{"retired remapped when events absent", "events.retired/0001", false, true, "events/0001"},
		{"scrub temp skipped", "events.scrub.7/0001", true, false, ""},
		{"discarded entry log skipped", "raft/log.discarded/0001", true, false, ""},
		{"bbolt restore temp skipped", "metadata/bbolt.db.restore.123", true, false, ""},
		{"bbolt compact temp skipped", "metadata/bbolt.db.compact.123", true, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keep, rel := datadir.CanonicalArchiveEntry(tt.rel, tt.eventsPresent)
			require.Equal(t, tt.wantKeep, keep)
			require.Equal(t, tt.wantRel, rel)
		})
	}
}

// TestRecoverScrubDirs_RollsBackAndReaps: the crashed-mid-swap rollback (events/
// absent, events.retired/ is the only log) plus reaping of scrub temps.
func TestRecoverScrubDirs_RollsBackAndReaps(t *testing.T) {
	root := t.TempDir()
	eventsDir := datadir.EventsDir(root)
	writeFileIn(t, datadir.RetiredDir(eventsDir), "0001", "the-only-log")
	require.NoError(t, os.MkdirAll(datadir.ScrubDir(eventsDir, 9), 0o700))

	require.NoError(t, datadir.RecoverScrubDirs(root))

	got, err := os.ReadFile(filepath.Join(eventsDir, "0001"))
	require.NoError(t, err)
	require.Equal(t, "the-only-log", string(got), "retired log rolled back to events/")
	require.NoDirExists(t, datadir.RetiredDir(eventsDir))
	require.NoDirExists(t, datadir.ScrubDir(eventsDir, 9))
}

// TestRecoverScrubDirs_DropsStaleRetiredWhenEventsPresent: a completed swap whose
// cleanup crashed leaves a stale events.retired/ next to the live events/; Open
// drops it, keeps events/.
func TestRecoverScrubDirs_DropsStaleRetiredWhenEventsPresent(t *testing.T) {
	root := t.TempDir()
	eventsDir := datadir.EventsDir(root)
	writeFileIn(t, eventsDir, "0001", "current")
	writeFileIn(t, datadir.RetiredDir(eventsDir), "0001", "stale")

	require.NoError(t, datadir.RecoverScrubDirs(root))

	require.NoDirExists(t, datadir.RetiredDir(eventsDir))
	got, err := os.ReadFile(filepath.Join(eventsDir, "0001"))
	require.NoError(t, err)
	require.Equal(t, "current", string(got))
}

// TestSweepBoltTempFiles removes orphaned bbolt swap temps, keeps the live db.
func TestSweepBoltTempFiles(t *testing.T) {
	md := t.TempDir()
	writeFileIn(t, md, "bbolt.db", "live")
	writeFileIn(t, md, "bbolt.db.restore.1", "orphan-restore")
	writeFileIn(t, md, "bbolt.db.compact.2", "orphan-compact")

	require.NoError(t, datadir.SweepBoltTempFiles(md))

	require.FileExists(t, datadir.BoltPath(md), "the live db is kept")
	require.NoFileExists(t, filepath.Join(md, "bbolt.db.restore.1"))
	require.NoFileExists(t, filepath.Join(md, "bbolt.db.compact.2"))
}

// TestRequireCompleteNodeDir: a complete node dir passes; dropping any canonical
// subtree fails with a message naming it (so a backup can't mint or restore a
// hollow node that boots with fresh-empty metadata).
func TestRequireCompleteNodeDir(t *testing.T) {
	complete := []string{
		"events/00000000000000000001",
		"raft/log/00000000000000000001",
		"raft/state/00000000000000000001",
		"metadata/bbolt.db",
	}
	require.NoError(t, datadir.RequireCompleteNodeDir(complete))

	for _, tt := range []struct{ name, drop, wantErr string }{
		{"missing metadata db", "metadata/bbolt.db", "metadata/bbolt.db"},
		{"missing event log", "events/00000000000000000001", "event log"},
		{"missing raft entry log", "raft/log/00000000000000000001", "raft entry log"},
		{"missing raft state log", "raft/state/00000000000000000001", "raft state log"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var files []string
			for _, p := range complete {
				if p != tt.drop {
					files = append(files, p)
				}
			}
			err := datadir.RequireCompleteNodeDir(files)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
