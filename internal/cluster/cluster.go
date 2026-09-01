// Package cluster is committed's domain vocabulary: Proposals (write
// requests), Actuals (committed facts), Entities, Types, Configurations,
// and the plugin contracts (Syncable, Ingestable, Database and their
// parsers). The engine implementing the operations over this vocabulary is
// *db.DB (internal/cluster/db); its HTTP surface is the engine's transport
// subpackage (internal/cluster/db/http), whose handlers hold the engine
// directly — there is deliberately no aggregated service interface here.
package cluster

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//go:generate protoc --go_out=paths=source_relative:. ./clusterpb/cluster.proto
