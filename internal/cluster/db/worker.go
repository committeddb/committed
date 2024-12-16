package db

func (db *DB) isNode(id string) bool {
	n := db.storage.Node(id)

	if n == 0 {
		return db.leaderState.IsLeader()
	}

	return n == db.ID()
}
