// Package datadir owns the on-disk layout of a committed node's data directory
// and the single definition of which entries are canonical node state versus
// transient recovery residue.
//
// Two subsystems must agree on this, exactly: the storage engine (wal.Open,
// which REMOVES or rolls back the residue before serving) and the backup tool
// (backup.Create, which must archive only the canonical state, never the
// residue). Keeping the layout names and the canonical-vs-residue taxonomy in one
// place means the two cannot drift — a rename here updates both, and adding a new
// residue kind is visible to both the sweep and the archive selector because they
// sit side by side.
//
// A data directory holds four canonical subtrees — raft/log (entry log),
// raft/state (state log), events (permanent event log), and metadata (BoltDB) —
// plus, after a crash, transient recovery residue a node's Open reaps:
//
//   - events.retired/                            the pre-scrub event log a scrub swap renamed aside
//   - events.scrub.<n>/                          a scrub rewrite's half-built temp log
//   - raft/log.discarded/                        an entry log a snapshot install superseded
//   - metadata/bbolt.db.{restore,compact}.<n>    orphaned bbolt swap temps
//
// events.retired/ and bbolt.db.restore.* can hold data a scrub already physically
// erased on the live node, so they must never be carried into an off-box backup
// (right-to-be-forgotten).
package datadir

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Canonical layout basenames.
const (
	eventsName   = "events"
	raftName     = "raft"
	entryLogName = "log"   // under raft/
	stateLogName = "state" // under raft/
	metadataName = "metadata"
	boltFileName = "bbolt.db"
)

// Recovery-residue name fragments.
const (
	retiredSuffix = ".retired"   // events -> events.retired
	scrubInfix    = ".scrub."    // events -> events.scrub.<n>
	discardSuffix = ".discarded" // raft/log -> raft/log.discarded

	// BoltRestorePrefix / BoltCompactPrefix name the full-DB temp files an atomic
	// bbolt swap (restore / compact) writes before renaming over bbolt.db. The
	// trailing '.' keeps them from ever matching the live bbolt.db.
	BoltRestorePrefix = boltFileName + ".restore."
	BoltCompactPrefix = boltFileName + ".compact."
)

// --- canonical path builders (keyed to match each caller's on-hand path) ---

// EventsDir returns the canonical event-log directory under the data root.
func EventsDir(root string) string { return filepath.Join(root, eventsName) }

// EntryLogDir returns the canonical raft entry-log directory under the data root.
func EntryLogDir(root string) string { return filepath.Join(root, raftName, entryLogName) }

// StateLogDir returns the canonical raft state-log directory under the data root.
func StateLogDir(root string) string { return filepath.Join(root, raftName, stateLogName) }

// MetadataDir returns the canonical BoltDB metadata directory under the data root.
func MetadataDir(root string) string { return filepath.Join(root, metadataName) }

// BoltPath returns the live BoltDB file inside a metadata directory.
func BoltPath(metadataDir string) string { return filepath.Join(metadataDir, boltFileName) }

// RetiredDir returns the retired (pre-scrub) event-log directory a scrub swap
// renames events/ to before publishing the rewritten log. eventsDir is EventsDir(root).
func RetiredDir(eventsDir string) string { return eventsDir + retiredSuffix }

// ScrubDir returns the temp directory a scrub rewrite builds the new log in.
// eventsDir is EventsDir(root).
func ScrubDir(eventsDir string, bound uint64) string {
	return fmt.Sprintf("%s%s%d", eventsDir, scrubInfix, bound)
}

// EntryLogDiscardDir returns where a snapshot install renames the superseded
// entry log aside. entryLogDir is EntryLogDir(root).
func EntryLogDiscardDir(entryLogDir string) string { return entryLogDir + discardSuffix }

// --- recovery: reap the residue at Open (the storage engine) ---

// RecoverScrubDirs repairs an interrupted scrub before the event log is opened.
// root is the data root (events/ lives at EventsDir(root)).
//
//   - events/ missing but events.retired/ present: a swap crashed after renaming
//     events out — roll back to the retired (pre-swap) log. The pending bound
//     re-drives an idempotent rewrite once the worker starts.
//   - Remove any leftover events.retired/ (a swap that completed but crashed
//     before cleanup) and any events.scrub.<n>/ temp dirs (a rewrite that crashed
//     before swapping).
func RecoverScrubDirs(root string) error {
	eventsDir := EventsDir(root)
	retiredDir := RetiredDir(eventsDir)

	if !dirExists(eventsDir) && dirExists(retiredDir) {
		if err := os.Rename(retiredDir, eventsDir); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(retiredDir); err != nil {
		return err
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh data dir; nothing to recover
		}
		return err
	}
	scrubPrefix := eventsName + scrubInfix // "events.scrub."
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), scrubPrefix) {
			if err := os.RemoveAll(filepath.Join(root, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// SweepBoltTempFiles removes orphaned bbolt.db.restore.* / bbolt.db.compact.*
// temp files from the metadata dir — the residue of a crash between a full-DB
// temp write (RestoreSnapshot / compactLocked) and its atomic rename. metadataDir
// is MetadataDir(root). A missing dir is a no-op (fresh node); the live bbolt.db
// never matches either prefix (both carry a trailing '.').
func SweepBoltTempFiles(metadataDir string) error {
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh data dir; nothing to sweep
		}
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, BoltRestorePrefix) || strings.HasPrefix(name, BoltCompactPrefix) {
			if err := os.RemoveAll(filepath.Join(metadataDir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- selection: which entries a backup archives (read-only twin of the sweep) ---

// CanonicalArchiveEntry decides whether the data-dir file at rel (a forward-slash
// path relative to the data root) belongs in a backup, and under what path. It is
// the read-only counterpart of the Open-time sweep above: a backup captures the
// canonical, post-recovery node state, never the residue — two forms of which
// (events.retired/, bbolt.db.restore.*) hold already-erased right-to-be-forgotten
// data that must not go off-box. eventsPresent is whether EventsDir exists.
//
// The one conditional mirrors RecoverScrubDirs: when events/ is absent and
// events.retired/ is present (a swap crashed after renaming events out),
// events.retired/ IS the only event log and Open rolls it back to events/, so the
// archive keeps it remapped to events/ rather than dropping it — otherwise the
// restore would have an empty log. That is not a leak: the erasure did not
// complete; the node still holds this log pending a re-scrub.
func CanonicalArchiveEntry(rel string, eventsPresent bool) (keep bool, archiveRel string) {
	seg := strings.Split(rel, "/")
	switch {
	case seg[0] == eventsName+retiredSuffix: // events.retired/
		if eventsPresent {
			return false, "" // events/ is canonical; the retired copy is a stale, swept-at-Open leak.
		}
		seg[0] = eventsName
		return true, strings.Join(seg, "/")
	case strings.HasPrefix(seg[0], eventsName+scrubInfix): // events.scrub.<n>/
		return false, ""
	case seg[0] == raftName && len(seg) > 1 && seg[1] == entryLogName+discardSuffix: // raft/log.discarded/
		return false, ""
	case seg[0] == metadataName && len(seg) > 1 &&
		(strings.HasPrefix(seg[1], BoltRestorePrefix) || strings.HasPrefix(seg[1], BoltCompactPrefix)):
		return false, "" // metadata/bbolt.db.{restore,compact}.<n>
	default:
		return true, rel
	}
}

// RequireCompleteNodeDir verifies that files — forward-slash paths relative to a
// node data directory — covers every canonical subtree a bootable node needs,
// returning an error naming the first missing one. A backup missing any would
// restore a hollow node that silently loses state: no metadata/bbolt.db makes the
// node's Open create a fresh EMPTY DB (every config, checkpoint, the applied
// index, and the conf state gone, presented as a clean boot); a missing event log
// loses committed data; a missing raft state log resets HardState. It guards
// backup Create (refuse to mint a hollow archive) and Restore (refuse to publish
// a hollow tree — catching a tampered or under-listing manifest).
func RequireCompleteNodeDir(files []string) error {
	boltPath := metadataName + "/" + boltFileName // metadata/bbolt.db
	required := []struct {
		desc  string
		match func(string) bool
	}{
		{eventsName + "/ (event log)", underDir(eventsName)},
		{raftName + "/" + entryLogName + "/ (raft entry log)", underDir(raftName + "/" + entryLogName)},
		{raftName + "/" + stateLogName + "/ (raft state log)", underDir(raftName + "/" + stateLogName)},
		{boltPath + " (metadata db)", func(p string) bool { return p == boltPath }},
	}
	for _, req := range required {
		if !slices.ContainsFunc(files, req.match) {
			return fmt.Errorf("data directory is missing %s — not a complete committed node directory", req.desc)
		}
	}
	return nil
}

// underDir returns a predicate matching forward-slash paths inside dir.
func underDir(dir string) func(string) bool {
	prefix := dir + "/"
	return func(p string) bool { return strings.HasPrefix(p, prefix) }
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
