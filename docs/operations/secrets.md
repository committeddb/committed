# Secret interpolation in configs

This runbook is for operators. It describes how to keep credentials —
database passwords, replication users, webhook tokens — out of the Raft
log and the bbolt store while still configuring databases, ingestables,
and syncables through the normal API.

## The problem

A database connection string carries a credential. Committed replicates
and persists *config* — the text you submit is written through Raft into
bbolt on every node — and anything left verbatim in that text is readable
by anyone with the bbolt file, a proposal dump, or an API response.

Committed has to store *config*; it does not have to store *secrets*.

### Enforced for connection-string passwords

Committed does not merely *recommend* `${VAR}` for a database or ingestable
`connectionString` — it **rejects a connection string whose password is written
as a literal**, at submit time (HTTP 400), so a plaintext credential can never be
proposed into the replicated log or a snapshot. A password given as `${VAR}` (or a
string with no password) is accepted; a literal password is refused with a message
pointing here. This applies only to the password position of `connectionString`:
hosts, usernames, database names, and every non-connection field are unaffected.

## The mechanism: `${VAR}` templates

Any string value in a database, ingestable, or syncable config may
contain `${VAR}` references. Each node expands them **at parse time**
against its own process environment. The expansion never leaves the
node:

- The template (`${VAR}`), not the resolved value, is what gets proposed
  into Raft and stored in bbolt.
- Every node resolves the reference locally from its own environment.
- A proposal dump or `GET` of the config shows the template, not the
  secret.

This means the secret lives in your orchestration layer — a Kubernetes
Secret, a Vault agent's `EnvironmentFile`, a systemd unit's
`EnvironmentFile=` — exactly where secret material is supposed to live.

### Grammar

| Syntax     | Meaning                                                       |
| ---------- | ------------------------------------------------------------- |
| `${NAME}`  | Expands to env var `NAME`. **Unset → rejected at submit (HTTP 400); degrades the config at startup/apply** (see Failure mode). |
| `$$`       | A literal `$` (escape). Use in a password like `p$$w0rd`.     |
| `$${NAME}` | A literal `${NAME}` (escape). Use when a config **value** must contain the literal text `${...}` — see "Escaping a literal `${...}`" below. |
| `$`        | Any other `$` is preserved verbatim. Bare `$VAR` is **not** expanded — `${...}` is required. |

A set-but-empty variable expands to the empty string (distinct from
unset).

### Escaping a literal `${...}` in a non-secret value

Interpolation runs on **every string value** in the config — not only
connection strings and auth headers, but user-data fields too: a
projection column default, a rule/mapping value, a webhook URL or body.
That is deliberate (it lets you reference a secret anywhere, e.g.
`${WEBHOOK_TOKEN}` inside a webhook body), but it means a value that
contains the literal characters `${...}` is treated as a reference and
expanded — or, if the named variable is unset, the whole config is
rejected/degraded with the usual missing-variable error.

If a value must contain a **literal** `${...}` that is not a secret
reference, escape the leading `$`:

```toml
# Wanted the literal string "${orderId}" as a projection default, not an
# env-var expansion:
default = "$${orderId}"   # stored/used as the literal "${orderId}"
```

The same applies to a literal double-dollar: write `$$$$` for a literal
`$$`. A lone `$` not followed by `$` or `{` (for example `$5`) is left
alone and needs no escaping.

### Example: MySQL database config

```toml
[database]
name = "orders"
type = "sql"

[sql]
dialect = "mysql"
connectionString = "mysql://app:${MYSQL_PASSWORD}@db.internal:3306/orders"
```

`${MYSQL_PASSWORD}` is resolved from the node's environment when the
config is parsed. The stored config keeps the literal
`${MYSQL_PASSWORD}` template.

## Operational rule: provision the env var on every node first

> **Set the secret on _every_ node — via a rolling restart — _before_ you
> propose a config that references it.** A node missing a referenced
> `${VAR}` stays up and in quorum, but it cannot build that config: the
> config is persisted cluster-wide and sits **degraded** on that node (the
> database / ingestable / syncable it configures is not active there)
> until you provide the variable and restart the node.

This is the single most important thing to understand about this
feature. Interpolation happens locally on each node, so a config that
references `${FOO}` is only safe to propose once **all** nodes already
have `FOO` in their environment. The correct order of operations is:

1. Add the variable to every node's environment and restart each node
   (rolling restart — environment is read at process start; there is no
   hot reload). For a secret rotation, this is also the procedure: update
   the env var and restart; you do **not** propose a new config.
2. Only then submit (or re-submit) the config that references it.

If you propose first, every node missing the variable persists the config
but marks it **degraded** — see Failure mode below. The node keeps
serving; the config just isn't active there until you fix its environment
and restart it.

## Failure mode: missing variable

A `${VAR}` whose variable is not set never becomes a silent empty
credential. What happens depends on where it is caught:

- **At submit time**, on the node that receives the API call (it
  interpolates against its own environment before proposing): the request
  is rejected with **HTTP 400** naming the missing variable. Nothing is
  proposed — the node you submit to fails fast, before anything is
  replicated.

- **At startup**, every persisted config is re-checked against the
  current environment before the node begins serving. A node missing a
  variable a stored config references **starts anyway, with that config
  degraded** — it logs a loud error and skips building the config's live
  object, but it joins the cluster and keeps serving everything else:

  ```
  ERROR  config persisted but a required ${VAR} secret is unset on this node
         (degraded); the node stays in quorum, fix the environment and
         restart to build it   {"kind": "database", "id": "orders",
         "error": "environment variable \"MYSQL_PASSWORD\" referenced in
         config is not set; provide it in this node's environment, or — if
         ${MYSQL_PASSWORD} is meant as literal config text — escape it as
         $${MYSQL_PASSWORD}"}
  ```

- **At apply time**, if a config is proposed (and committed) while a
  *running* node is missing the variable, that node **persists the config
  and marks it degraded** — it does **not** crash. The config bytes are
  identical on every node (they are replicated); only the local build of
  the connection/worker is deferred on the node that can't resolve the
  secret.

### Why a missing secret degrades the config

Interpolation reads the local environment, so a missing `${VAR}` is a
*node-local* condition: the config itself is valid and replicated
identically on every node. A node-local gap must not take a node out of
quorum — if it did, a secret rolled to some nodes but not all could drop
the whole follower set at once, and for a freshly-committed config that is
quorum loss. So persisting the config (deterministic, identical on every
node) always succeeds; only the local construction — the part that reads
the environment — is deferred. The config is persisted everywhere and is
degraded only on the nodes that can't resolve the secret.

This covers any node-local build failure, missing secret or otherwise —
**including config bytes that don't even decode**: those are recorded as a
degraded config too and the node stays in quorum, not a node halt. The only
corruption that *stops* a node is a bad checksum on a **WAL entry** itself
(`ErrCorruptEntry` at startup — see [rebuild.md](rebuild.md)), a lower layer than
any individual config's contents.

### Observing and recovering a degraded config

- **Metric:** `committed_config_build_errors` is a per-node gauge: the
  number of configs persisted on that node but not buildable there.
  Non-zero means "a degraded config on this node," **not** "a down node."
  Alert on it.
- **Logs:** each degraded config emits the `ERROR` above, naming the kind,
  id, and the unset variable.
- **Recover:** provide the variable in the node's environment and
  **restart** the node (environment is read at process start; there is no
  hot reload). On restart the config builds and the gauge returns to zero.
  You do **not** re-submit the config.

## Kubernetes

Put the secret in a `Secret` and project it into the container
environment with `envFrom`. The same Deployment template then works for
every node:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: committed-db-secrets
type: Opaque
stringData:
  MYSQL_PASSWORD: "s3cr3t"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: committed
spec:
  template:
    spec:
      containers:
        - name: committed
          image: committed:latest
          envFrom:
            - secretRef:
                name: committed-db-secrets
```

Submit the config with the `${MYSQL_PASSWORD}` template; each pod
resolves it from the projected environment.

## systemd

Keep the secret in a root-owned `EnvironmentFile` (mode `0600`) and
reference it from the unit:

```ini
# /etc/committed/secrets.env  (chmod 0600, chown root:root)
MYSQL_PASSWORD=s3cr3t
```

```ini
# committed.service
[Service]
EnvironmentFile=/etc/committed/secrets.env
ExecStart=/usr/local/bin/committed node
```

## What's not in scope

- **Vault / AWS Secrets Manager integration.** Env-var interpolation
  covers the common case; a dedicated secrets-manager adapter is a
  separate piece of work. Both tools can render an `EnvironmentFile` or
  inject env vars today, which is enough to use this feature.
- **Encrypting bbolt at rest.** Templates, not secrets, are stored — so
  there is nothing secret in bbolt to encrypt for this purpose.
- **Rotation without restart.** Rotation is "update the env var, restart
  the node."
- **File-sourced secrets** (`$FILE{/path}`). Could be added as a second
  token type later if operators ask for it.
