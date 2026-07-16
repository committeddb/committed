package cluster

import (
	"errors"
	"fmt"
	"strings"
)

// Database represents a query database
//
//counterfeiter:generate . Database
type Database interface {
	Close() error
	GetType() string
}

type DatabaseStorage interface {
	Database(id string) (Database, error)
}

// DatabaseConfigChangeValidator is the optional Database extension that answers
// whether replacing the currently-built database with this one would change the
// underlying connection pool. The propose path uses it to decide whether a
// re-POST would swap the *sql.DB out from under any syncable that captured it:
// syncables resolve storage.Database(id) once, at parse time, and are NOT rebuilt
// when a database config re-applies, so a pool swap would either leak the old
// pool or leave the syncable on a closed handle. A pool-preserving change
// (identical connection) returns nil; a pool-changing one returns non-nil. It
// never touches the network.
type DatabaseConfigChangeValidator interface {
	ValidateReplace(prior Database) error
}

// ErrDatabaseConnectionChanged is the sentinel a DatabaseConfigChangeValidator
// returns from ValidateReplace when the connection pool would change. The propose
// path only escalates it to a 409 (DatabaseInUseError) when syncables actually
// reference the database; with no dependents the swap is safe and allowed.
var ErrDatabaseConnectionChanged = errors.New("database connection changed")

// databaseInUseCode is the stable machine-readable code a deploy pipeline
// branches on to drive the tear-down-syncables-then-re-POST recovery without
// scraping the message. Surfaced generically via RebuildRequiredError.Code.
const databaseInUseCode = "database_in_use_requires_syncable_teardown"

// DatabaseInUseError reports that a re-POST would change a database's connection
// pool while one or more syncables still reference it. A syncable captures the
// destination *sql.DB pool at parse time (storage.Database(id)) and a database
// re-apply does not rebuild syncables, so swapping the pool in place would leave
// those syncables writing to a closed handle. The safe recovery is to tear down
// the dependent syncables first (DELETE /v1/syncable/{id}), re-POST the database,
// then recreate them. It implements cluster.RebuildRequiredError so the HTTP
// layer renders it as 409 + code + structured details without importing this
// decision.
type DatabaseInUseError struct {
	// DatabaseID is the id of the database whose connection change was rejected.
	DatabaseID string `json:"database"`
	// DependentSyncables are the syncables that reference DatabaseID and so must
	// be torn down before the connection can change.
	DependentSyncables []DependentSyncable `json:"dependentSyncables"`
}

func (e *DatabaseInUseError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b,
		"database %q connection change will not be applied in place: %d syncable(s) reference this database and captured its connection pool when they were built, and a database re-POST does not rebuild them. Tear down the dependent syncables (DELETE /v1/syncable/{id}), re-POST the database, then recreate them.",
		e.DatabaseID, len(e.DependentSyncables))
	if len(e.DependentSyncables) > 0 {
		names := make([]string, 0, len(e.DependentSyncables))
		for _, d := range e.DependentSyncables {
			if d.Name != "" {
				names = append(names, fmt.Sprintf("%s (%s)", d.Name, d.ID))
			} else {
				names = append(names, d.ID)
			}
		}
		fmt.Fprintf(&b, " syncables to rebuild: %s.", strings.Join(names, ", "))
	}
	return b.String()
}

// Code implements cluster.RebuildRequiredError.
func (e *DatabaseInUseError) Code() string { return databaseInUseCode }

// Details implements cluster.RebuildRequiredError: the exported fields (json
// tags above) are the machine-readable 409 payload.
func (e *DatabaseInUseError) Details() any { return e }

// DatabaseParser will parse a config document into a Database
//
//counterfeiter:generate . DatabaseParser
type DatabaseParser interface {
	Parse(c *ParsedConfig) (Database, error)
}

// EntityKindRevision (version-stored config with rollback, retained — see
// typeType).
var databaseType = registerSystemType(&Type{
	ID:         "4698b77e-9a7c-41a2-aae4-984da0cd33c1",
	Name:       "InternalDatabaseParser",
	Version:    1,
	EntityKind: EntityKindRevision,
})

func IsDatabase(id string) bool {
	return id == databaseType.ID
}

func NewUpsertDatabaseEntity(c *Configuration) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(databaseType, []byte(c.ID), bs), nil
}
