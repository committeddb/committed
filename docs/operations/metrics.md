# Metrics & observability

committed exports its metrics by **OTLP push**, not a scrape endpoint. There is no
`/metrics` handler. Each node pushes to an [OpenTelemetry
Collector](https://opentelemetry.io/docs/collector/) over gRPC on a periodic
interval; the collector fans out to your backend (Prometheus, Datadog, Grafana
Cloud, …). This keeps committed free of any backend-specific server and lets one
collector fan in an entire cluster.

## Enabling metrics

Metrics are **off by default** — when the switch below is unset, every metric call
is a no-op with zero overhead. Set the standard OpenTelemetry endpoint env var to
turn them on:

| Env var | Purpose |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector address, e.g. `http://otel-collector:4317`. **Setting this enables metrics.** Unset = disabled. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` (default) or `http/protobuf`. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Extra headers (auth), e.g. `authorization=Bearer <token>`. |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` to skip TLS to the collector (test/lab only). |

committed uses the standard OTLP SDK, so any of the [OTLP exporter env
vars](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) apply. The
service name is reported as `committed`.

## Bridging to Prometheus

If you alert in Prometheus, run a collector that receives OTLP and exposes a scrape
endpoint Prometheus reads (committed itself is never scraped):

```yaml
# otel-collector.yaml
receivers:
  otlp:
    protocols:
      grpc:            # committed pushes here (OTEL_EXPORTER_OTLP_ENDPOINT)
exporters:
  prometheus:
    endpoint: 0.0.0.0:9464   # Prometheus scrapes the COLLECTOR here, not committed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

### Metric names: dots vs underscores

Instruments are named with dots in committed (`committed.sync.lag`). The
OTLP→Prometheus conversion replaces dots with underscores, so the same metric is
`committed_sync_lag` in a Prometheus/PromQL context. Runbook alert expressions use
the underscore form; the catalog below lists the source (dotted) names.

## Metric catalog

Labels are shown in `{braces}`.

### Consensus, apply & log

| Metric | What it tells you |
|---|---|
| `committed.leader` | 1 on the node that is currently raft leader, 0 elsewhere. |
| `committed.leader.transitions.observed` | Count of leader changes this node has observed (churn signal). |
| `committed.proposals` | Proposals submitted to raft, by `{kind}` (user / config / index / position). |
| `committed.propose.duration` | Latency of a propose→commit round-trip (histogram). |
| `committed.propose.fail_fast.lost` | Proposals abandoned because a leader change lost them. |
| `committed.propose.fail_fast.unknown` | Proposals that returned an unknown/uncommitted outcome. |
| `committed.apply.duration` | Time to apply one committed entry to the state machine (histogram). |
| `committed.apply.index` | Highest raft index this node has applied. |
| `committed.read_index.duration` | Latency of a linearizable read-index confirmation (histogram). |
| `committed.first.index` | Oldest raft index still retained in the raft log. |
| `committed.last.index` | Newest raft index in the raft log. |

### Sync (syncable → external sink)

| Metric | What it tells you |
|---|---|
| `committed.sync.lag` | Per-`{syncable_id}` gap between the log head and what the syncable has delivered. |
| `committed.sync.duration` | Time to apply one Actual to a sink (histogram). |
| `committed.sync.errors` | Sync errors, by `{syncable_id}`. |
| `committed.sync.stuck` | 1 when a `{syncable_id}` worker has been blocked past the stuck threshold. **Alert on this.** |
| `committed.sync.breaker_trips` | A syncable's consecutive-permanent-error breaker tripped (dead-lettering en masse). |
| `committed.sync.bump.duration` | Latency of the post-sync checkpoint bump (histogram). |
| `committed.sync.last_error.timestamp` | Unix time of a syncable's most recent error. |
| `committed.sync.rules_unmatched` | Rows a syncable's rules matched no case for (dropped by config). |

### Ingest (CDC source → topic)

| Metric | What it tells you |
|---|---|
| `committed.ingest.lag` | Per-`{ingestable_id}` replication lag from the source. |
| `committed.ingest.errors` | Ingest errors, by `{ingestable_id}`. |
| `committed.ingest.frozen` | 1 when an ingestable is frozen (supervisor gave up); needs operator attention. |
| `committed.ingest.dedup_skipped` | Source events skipped as already-consumed (effectively-once dedup). |
| `committed.ingest.restarts` | Ingest worker restarts (reconnect/backoff churn). |
| `committed.ingest.supervisor_giveups` | Times the ingest supervisor exhausted its retry budget and parked the worker. |
| `committed.ingest.position.bump.duration` | Latency of the ingest position checkpoint (histogram). |
| `committed.ingest.last_error.timestamp` | Unix time of an ingestable's most recent error. |

### Disk & write admission

| Metric | What it tells you |
|---|---|
| `committed.disk.free_bytes` | Free space on the data volume. |
| `committed.disk.free_percent` | Free space as a percent of the volume. |
| `committed.disk.state` | This node's disk level, `{level=ok\|warn\|critical\|full}`. |
| `committed.disk.cluster_state` | The cluster-effective level the write-admission gate is enforcing. |
| `committed.disk.leadership_transfers` | Disk-pressure leadership hand-offs. |
| `committed.write.admitted` | 1/0: does this node's gate admit user-data writes right now. |
| `committed.write.admission_reason` | Exactly one `{reason=ok\|leader_disk\|quorum_at_risk\|cluster_reject\|local_fallback}` is 1. |

### Storage integrity, migration & workers

| Metric | What it tells you |
|---|---|
| `committed.wal.corrupt_entries` | CRC-detected corrupt WAL/event-log entries. Any non-zero value is a rebuild signal (see [rebuild.md](rebuild.md)). |
| `committed.type.migration.duration` | Time to run a type-version migration transform (histogram). |
| `committed.type.migration.errors` | Type-migration transform failures. |
| `committed.worker.running` | Sync/ingest workers currently running on this node. |
| `committed.worker.replaces` | Worker replacements (config re-apply, rebuild). |
| `committed.config.build_errors` | Configs this node persisted but could not build (degraded — usually a missing `${VAR}`). Diagnose with `GET /v1/node/status`. |
| `committed.entity_kind.misuse` | Entities whose declared kind doesn't match how they're used (config warning). |
| `committed.http.request_too_large` | HTTP requests rejected for exceeding the body-size limit. |

## Related runbooks

- [stuck-syncables.md](stuck-syncables.md) — `committed.sync.stuck`
- [disk-limits.md](disk-limits.md) — the disk / write-admission gauges
- [upgrade.md](upgrade.md) — `committed.wal.corrupt_entries`
