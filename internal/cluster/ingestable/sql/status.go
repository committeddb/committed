package sql

import (
	"slices"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// SnapshotTableStatus reports each configured table's place in the initial
// snapshot, shared by both dialects since the snapshot-progress proto is shared.
//
// During the snapshot phase (progress != nil) it reflects the live cursor: a
// table in CompletedTables reads Complete, the rest carry their keyset cursor
// from LastPkByTable. Once the snapshot is done (progress == nil — the streaming
// phase no longer checkpoints snapshot progress) every configured table reads
// Complete, since reaching streaming means the snapshot covered them all.
func SnapshotTableStatus(config *Config, progress *dialectpb.SnapshotProgress) []cluster.TableSnapshotStatus {
	out := make([]cluster.TableSnapshotStatus, 0, len(config.Tables))
	for _, t := range config.Tables {
		st := cluster.TableSnapshotStatus{Table: t}
		switch {
		case progress == nil:
			st.Complete = true
		case slices.Contains(progress.CompletedTables, t):
			st.Complete = true
		default:
			st.LastKey = progress.LastPkByTable[t]
		}
		out = append(out, st)
	}
	return out
}
