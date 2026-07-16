package db

import (
	"context"
	"slices"

	"github.com/committeddb/committed/internal/cluster"
)

func (db *DB) AddDatabaseParser(name string, p cluster.DatabaseParser) {
	db.parser.AddDatabaseParser(name, p)
}

func (db *DB) ProposeDatabase(ctx context.Context, c *cluster.Configuration) error {
	name, database, err := db.parser.ParseDatabase(c.MimeType, c.Data)
	if err != nil {
		return cluster.NewConfigError(err)
	}
	c.Name = name

	// Guard: a re-POST (or rollback) that changes the connection pool while
	// syncables still reference this database would swap the *sql.DB out from under
	// them — they captured it at build time and a database apply does not rebuild
	// them. Reject with a 409 naming the syncables to tear down first. Runs on the
	// freshly-built `database` as the candidate; the handle is closed regardless
	// below (it was only built to validate).
	guardErr := db.guardDatabaseConfigChange(c.ID, database)
	closeErr := database.Close()
	if guardErr != nil {
		return guardErr
	}
	if closeErr != nil {
		return closeErr
	}

	e, err := cluster.NewUpsertDatabaseEntity(c)
	if err != nil {
		return err
	}
	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

// guardDatabaseConfigChange rejects a database config change that would swap the
// connection pool out from under a syncable that captured it. The database owns
// the "did my connection change" decision (cluster.DatabaseConfigChangeValidator,
// which compares connection strings without touching the network); this layer
// owns the topology (which syncables reference which database) and only escalates
// a pool change to a 409 when syncables actually depend on it — with none, the
// apply path safely closes the old pool and builds the new one.
//
// Fail-open, mirroring guardIngestableConfigChange: if the new database doesn't
// validate replacements, there is no prior config, or the prior config can't be
// rebuilt on this node, the guard allows the change. It is a signpost toward
// tear-down-and-recreate, not a correctness gate — blocking a deploy on an
// un-parseable old config would be worse than the leak it guards against.
func (db *DB) guardDatabaseConfigChange(id string, next cluster.Database) error {
	validator, ok := next.(cluster.DatabaseConfigChangeValidator)
	if !ok {
		return nil // this database kind can't compare connections — nothing to guard
	}

	prior := db.currentDatabaseConfig(id)
	if prior == nil {
		return nil // first POST for this id — no pool to preserve
	}

	_, priorDB, err := db.ParseDatabase(prior.MimeType, prior.Data)
	if err != nil {
		return nil // prior config not buildable on this node — fail open
	}
	defer func() { _ = priorDB.Close() }()

	if validator.ValidateReplace(priorDB) == nil {
		return nil // identical connection — apply keeps the existing pool, no swap
	}

	// The connection would change; that only breaks something if syncables
	// captured this pool. With no dependents the apply path closes the old pool
	// and builds the new one safely.
	deps := db.syncablesReferencingDatabase(id)
	if len(deps) == 0 {
		return nil
	}
	return &cluster.DatabaseInUseError{DatabaseID: id, DependentSyncables: deps}
}

// currentDatabaseConfig returns the currently-persisted database configuration
// for id, or nil if none exists (or the list can't be read). Mirrors
// currentIngestableConfig.
func (db *DB) currentDatabaseConfig(id string) *cluster.Configuration {
	cfgs, err := db.storage.Databases()
	if err != nil {
		return nil
	}
	for _, c := range cfgs {
		if c.ID == id {
			return c
		}
	}
	return nil
}

// syncablesReferencingDatabase returns the persisted syncables that write to the
// database id — the ones that captured its connection pool at build time, so a
// rejected connection change can tell the operator exactly what to tear down.
// Databases are read from each syncable's config alone (no Init / no destination
// I/O). Best-effort: a syncable whose config won't parse is skipped rather than
// failing the whole enumeration (it can only undercount in an already-degraded
// case). Mirrors syncablesConsumingTopic.
func (db *DB) syncablesReferencingDatabase(id string) []cluster.DependentSyncable {
	cfgs, err := db.storage.Syncables()
	if err != nil {
		return nil
	}
	var deps []cluster.DependentSyncable
	for _, c := range cfgs {
		dbs, err := db.parser.SyncableDatabases(c.MimeType, c.Data)
		if err != nil {
			continue
		}
		if slices.Contains(dbs, id) {
			deps = append(deps, cluster.DependentSyncable{ID: c.ID, Name: c.Name})
		}
	}
	return deps
}

func (db *DB) ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error) {
	return db.parser.ParseDatabase(mimeType, data)
}

func (db *DB) Databases() ([]*cluster.Configuration, error) {
	return db.storage.Databases()
}

func (db *DB) DatabaseVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.DatabaseVersions(id)
}

func (db *DB) DatabaseVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.DatabaseVersion(id, version)
}
