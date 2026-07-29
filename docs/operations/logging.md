# Logging and personal data

committed writes structured (JSON) logs to standard error; collecting, forwarding,
and retaining them is yours to configure. This page covers what those logs
contain, why node-local logs deliberately hold customer data, and the retention
responsibility that follows for right-to-be-forgotten.

## Node logs hold customer data — by design

committed draws its redaction boundary at the point data *leaves a node*.
[Secrets](secrets.md) and any record that crosses that boundary — an HTTP error
body, a replicated or cluster-visible record — carry only a redacted, PII-free
classifier. Node-local logs are the other side of that boundary: they deliberately
keep the **full, unredacted detail** an operator needs to diagnose a failure on
the node where it happened.

The one place customer data reaches a log is a **failed sink apply**. When a
syncable's write to your destination database is rejected, committed logs the
driver's error verbatim so you can see *why* — and a driver error routinely echoes
the offending row: a PostgreSQL unique/foreign-key violation includes
`Key (col)=(value)`, a MySQL duplicate-key error names the value. That value is the
entity's key or data. A recovered panic can, rarely, carry a value the same way.
**Treat your node logs as potentially containing customer personal data.**

This is a deliberate diagnosability choice, not an oversight: the full reason a
sink rejected a row is exactly what an operator needs, and it stays on the node
rather than crossing the boundary into an API response or a replicated record.

## What is not in the logs

The vector is narrow — logs are not a copy of your data:

- **Secrets are redacted even node-local.** Database passwords and webhook URLs
  never appear; `${VAR}` connection strings render without credentials. See
  [Secrets](secrets.md).
- **Only error-path values.** A value reaches a log only when a row's apply
  *fails*. Rows that apply cleanly are never logged. Logs hold a sliver of
  error-triggering values, not your dataset — contrast a [backup](backup.md),
  which is everything.
- **No statement text or row dumps.** DDL is logged as a length, not its text; a
  row committed cannot decode or marshal is logged with the *reason* (an encoding
  or JSON error), never its bytes; metric labels carry no data or credentials.

## Shared responsibility: logs and right-to-be-forgotten

A right-to-be-forgotten delete is scrubbed from the event log within the scrub
window, and the short-lived consensus buffer self-clears (~1 hour — see
[event-log architecture](../event-log-architecture.md)). **The scrubber does not
reach your logs.** A value written to a log line *before* an erasure request stays
in whatever store you forwarded that line to, exactly like a [backup](backup.md)
taken before the delete — committed cannot reach into your log pipeline to remove
it, and it keeps no ledger of erased subjects to tell you which lines to purge.

The "forgotten" window for logs is therefore the **retention of your log store**,
not the scrub latency. So:

- **Bound log retention to your RTBF obligations.** Expire old logs on a schedule;
  **≤30 days is a reasonable default** — set it to whatever your regime requires.
- **Treat the log pipeline as a PII store.** Your forwarder and central store
  (SIEM, log service, archive) are in scope for retention and erasure the same way
  backups are — not an indefinite archive. Access-control them as customer data.
- **Keep accumulation down.** A wedged syncable logs the row it cannot apply;
  resolve it via the [stuck-syncable](stuck-syncables.md) workflow (skip or replay
  the bad record) so the same value is not re-logged, and let retention age out
  what remains.

committed gives you the diagnosable detail on the node and the redaction at the
boundary; bounding log retention and keeping your log pipeline within your
compliance regime is the operator's responsibility — the same shared-responsibility
split as [backups](backup.md).
