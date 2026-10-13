# Committed

[![CI](https://github.com/committeddb/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/committeddb/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed distributed commit log that is its own source of truth — write events in, sync them out to systems built for querying.

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use add-ons, or third-party software to aid in your read activities, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a database designed for querying.

## What makes it distinctive

- **vs. an event store's projection layer**: keep your write side. Committed reads the events table you already have in Postgres, MySQL, or SQL Server and [builds the read models](docs/recipes/event-store-read-models.md): quickly, without downtime, failing loudly, and with no plumbing code.
- **vs. Kafka**: a single replicated log instead of partitioned topics, with built-in *output-side* projection (syncables) rather than consumer offsets. Configs live in the log, not ZooKeeper/KRaft.
- **vs. etcd**: same Raft substrate, but append-only log semantics instead of KV — and a worker model for ingest/sync that etcd doesn't have.
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the destination.

## Documentation

- [Quickstart](docs/quickstart.md) — one `docker compose up` takes a normalized
  movie catalog to a single denormalized table you query with no joins.
- [Running Committed](docs/running.md) — the binary or the container, a single
  node or a three-node cluster, and the `COMMITTED_*` variables that configure
  a node.
- [Developing Committed](docs/development.md) — build from source, the
  repository layout, the test suites, lint, and generated code.
- [Architecture](docs/architecture.md) — how the pieces fit, the node's storage
  layout, and the design references.
- [API tour](docs/api-tour.md) — a short example for each endpoint.
- [Read models](docs/read-models.md) — fold topics into the current-state
  tables you query: rules, multi-source rows, aggregates, enrichment, and
  stages.

## Concepts

- **[Type](docs/types.md)** — schema/metadata for a topic's payload. Identified by ID; supports explicit-migration versioning.
- **[Proposal](docs/proposals.md)** — a write *request*: one or more entities (each tagged with a Type ID) offered to the log together. You propose a Proposal; consensus decides its fate. It has no place in the order yet.
- **[Actual](docs/actuals.md)** — a committed fact: the Proposal that consensus ordered and wrote to the log at a fixed Index. You propose Proposals, you *sync* Actuals — a Syncable is handed Actuals (in Index order), never Proposals.
- **[Database](docs/databases.md)** — connection config for an external SQL syncable that syncables reference.
- **[Ingestable](docs/ingestables.md)** — pulls data into the log from an external source.
- **[Syncable](docs/syncables.md)** — projects committed Actuals out to an external system, either as faithful per-table mirrors (`sql`) or by transforming and folding topics into the denormalized shape you query — multi-topic BFF rows, collection aggregates, cross-topic enrichment.

## Recipes

- **[Read models from your event store](docs/recipes/event-store-read-models.md)** — keep your event store; Committed ingests its events table and builds the read models.

## Runbooks

### Securing

- [Authentication](docs/operations/authentication.md) — turn on the bearer
  token, API TLS, and peer mTLS.
- [Secrets](docs/operations/secrets.md) — keep database passwords and tokens out
  of the log with `${VAR}` config interpolation.

### Monitoring and capacity

- [Metrics & observability](docs/operations/metrics.md) — turn on metrics (OTLP
  push, no scrape endpoint), bridge to Prometheus, and the full metric catalog.
- [Stuck syncables](docs/operations/stuck-syncables.md) — spot a syncable that's
  wedged or falling behind, skip a bad record, and replay it after a fix.
- [Disk limits](docs/operations/disk-limits.md) — how the cluster protects
  itself as disks fill, the thresholds and metrics to watch, and what to do in
  an incident.
- [Memory](docs/operations/memory.md) — what drives a node's memory, and how to
  size and bound it.
- [Performance envelope](docs/operations/performance.md) — the
  write/apply/catch-up rates to plan around, and how to re-measure them on your
  hardware.
- [HTTP limits](docs/operations/http-limits.md) — the proposal-size cap and HTTP
  server timeouts.

### Changing the cluster

- [Cluster membership](docs/operations/membership.md) — add, remove, and grow
  nodes safely (learner → catch up → promote), and read cluster state.
- [Zone-pinned syncables](docs/operations/zones.md) — keep sync egress inside a
  zone, and what a strict pin does when its node is down.
- [Rolling upgrades](docs/operations/upgrade.md) — upgrade the cluster
  node-by-node with no downtime, and roll back if needed.

### Recovering

- [Rebuilding a node](docs/operations/rebuild.md) — a node that fell behind
  catches up by itself; replace one whose disk or state cannot be trusted.
- [Backup and restore](docs/operations/backup.md) — archive a node, stopped or
  live, to a portable tarball for disaster recovery and total-loss rebuilds.
- [Safe mode](docs/operations/safe-mode.md) — boot with workers held to break a
  crashloop: inspect and delete/fix the offending config over the API, then
  restart normally.
- [Graceful shutdown](docs/operations/shutdown.md) — what `SIGTERM` does and how
  to tune the drain deadline.

### Data and compliance

- [Right-to-be-forgotten erasure](docs/operations/rtbf.md) — issue subject
  deletes, expedite the physical scrub, verify completion per node, and what can
  delay the last identifier's erasure.
- [Logging & personal data](docs/operations/logging.md) — what Committed writes
  to its logs, why node logs carry customer data by design, and bounding log
  retention to your right-to-be-forgotten obligations.

## License

Committed is licensed under the [Apache License 2.0](LICENSE); see
[NOTICE](NOTICE). Third-party license texts are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
