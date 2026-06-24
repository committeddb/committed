package db_test

import (
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/httptransport"
)

// db no longer imports a concrete transport — the composition root injects one
// (see WithTransportFactory). For the test suite, register the real HTTP
// transport once here so every db.New / NewRaft call site across this package
// gets it without wiring a factory by hand. Runs before any test (init).
func init() {
	db.SetTestTransportFactory(httptransport.Factory())
}
