package db

// isNode reports whether THIS node owns syncable/ingestable id's work.
// Ownership resolves in three tiers:
//
//   - a zone-pinned syncable (config carries `zone`, the cluster feature
//     level permits resolution): owned by the lowest-id current member
//     announcing that zone; when NO member announces it, the strict pin is
//     unsatisfiable and NOBODY owns it — the worker stalls loudly rather
//     than silently falling back to the leader (see db/zone.go);
//   - an explicit storage-pinned node (Storage.Node != 0, unused today);
//   - otherwise the leader (Storage.Node == 0 — every pre-zone config).
func (db *DB) isNode(id string) bool {
	if zone, active, owner := db.syncableZonePin(id); zone != "" && active {
		if owner == 0 {
			return false // strict pin unsatisfiable: nobody serves; lag, never loss
		}
		return owner == db.ID()
	}

	n := db.storage.Node(id)

	if n == 0 {
		return db.leaderState.IsLeader()
	}

	return n == db.ID()
}
