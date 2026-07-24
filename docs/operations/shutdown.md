# Graceful shutdown

This runbook is for operators. It describes how `committed` responds to
`SIGINT` and `SIGTERM`, what the graceful-shutdown path does, and how to
tune its deadline for your orchestrator.

## What happens on a signal

When a `committed` node receives `SIGINT` (Ctrl-C, `kill -INT`) or
`SIGTERM` (`kill`, `systemctl stop`, a Kubernetes pod eviction), it
runs the graceful-shutdown path:

1. The HTTP server stops accepting new connections and waits for
   in-flight requests to finish.
2. The database layer (`db.Close`) is called: sync/ingest worker
   goroutines are canceled and drained, leadership is handed off if this
   node is the raft leader (see below), raft is stopped cleanly, and the
   WAL is closed.
3. The process exits `0`.

Each phase emits a structured log line you can grep for in an
incident:

- `shutdown.signal_received` — signal caught, drain starting.
- `shutdown.http_closed` — HTTP drain finished within the deadline.
- `shutdown.http_timeout` — HTTP drain exceeded the deadline (see
  below).
- `shutdown.db_closed` — raft and workers drained, WAL closed.
- `shutdown.done` — exit.

If the HTTP drain does not finish before the deadline, the server is
hard-closed (in-flight responses are dropped), `db.Close` still runs so
raft + WAL shut down cleanly, and the process exits `1` so the
orchestrator can see that the graceful window was blown.

## Configuring the deadline

The **total** graceful-shutdown budget is set by the
`COMMITTED_SHUTDOWN_TIMEOUT` environment variable — it bounds the whole path:
the HTTP drain, then the worker drain and leadership hand-off in `db.Close`,
all clamped to this one deadline (only the final raft stop, normally instant,
runs after it). It accepts Go duration syntax (`30s`, `45s`, `1m`, `2m30s`,
...). When unset, the default is **30 seconds**, chosen to fit inside
Kubernetes' default `terminationGracePeriodSeconds: 30` — if your pod spec
raises that value, raise `COMMITTED_SHUTDOWN_TIMEOUT` to match so `SIGKILL`
doesn't preempt the graceful path.

```bash
COMMITTED_SHUTDOWN_TIMEOUT=45s ./committed node --id 1 ...
```

An unparseable value (`COMMITTED_SHUTDOWN_TIMEOUT=forever`) or a
non-positive duration (`0s`) logs a warning and falls back to the
default. The deadline is never silently disabled — an unbounded drain
would just stall the orchestrator and eventually earn a `SIGKILL`.

## Kubernetes

For a typical Deployment or StatefulSet:

```yaml
spec:
  terminationGracePeriodSeconds: 45
  containers:
    - name: committed
      env:
        - name: COMMITTED_SHUTDOWN_TIMEOUT
          value: "30s"
      lifecycle:
        preStop:
          exec:
            # Optional: give endpoints time to deregister before the
            # SIGTERM arrives, so new requests stop arriving *before*
            # the drain starts.
            command: ["sleep", "5"]
```

Leave `terminationGracePeriodSeconds` at least a few seconds larger
than `COMMITTED_SHUTDOWN_TIMEOUT` + any `preStop` sleep so the
graceful path can finish before Kubernetes sends `SIGKILL`.

## systemd

```ini
[Service]
ExecStart=/usr/local/bin/committed node --id 1 ...
Environment=COMMITTED_SHUTDOWN_TIMEOUT=30s
# Give the graceful path its full deadline before systemd escalates
# to SIGKILL. Keep this slightly larger than COMMITTED_SHUTDOWN_TIMEOUT.
TimeoutStopSec=45
KillSignal=SIGTERM
```

## Leadership handoff

If the node is the raft leader when it shuts down, `db.Close` hands
leadership to the most caught-up voter before stopping raft, so the
cluster doesn't have to run a full election. The handoff is bounded: if
no caught-up voter is reachable, or it doesn't complete within a few
seconds, the node stops anyway and the remaining peers elect a new leader
the usual way. A follower shutting down skips this entirely. This is what
keeps a rolling upgrade from stalling writes for ~1 second each time the
leader restarts.

## What is *not* done on shutdown

- **In-flight proposal drain.** HTTP requests are drained (they finish
  or are dropped by the deadline), but proposals already sent to raft
  are not individually awaited — `db.Close` cancels the contexts that
  are waiting on apply, so any blocked `POST /v1/proposal` returns an
  error to the caller instead of hanging until the deadline.
- **SIGHUP reload.** There is no dynamic config reload; a config
  change today still requires a full restart.
