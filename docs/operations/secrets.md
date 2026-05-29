# Secret interpolation in configs

This runbook is for operators. It describes how to keep credentials —
database passwords, replication users, webhook tokens — out of the Raft
log and the bbolt store while still configuring databases, ingestables,
and syncables through the normal API.

## The problem

Database connection strings and other credentials used to live verbatim
in the TOML/JSON you submit, and that text is replicated through Raft and
persisted in bbolt on every node. Anyone with read access to the bbolt
file, a proposal dump, or an API response could read the secret.

Committed has to store *config*; it does not have to store *secrets*.

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

| Syntax    | Meaning                                                        |
| --------- | ------------------------------------------------------------- |
| `${NAME}` | Expands to env var `NAME`. **Unset → hard error** (see below). |
| `$$`      | A literal `$` (escape). Use in a password like `p$$w0rd`.      |
| `$`       | Any other `$` is preserved verbatim. Bare `$VAR` is **not** expanded — `${...}` is required. |

A set-but-empty variable expands to the empty string (distinct from
unset).

### Example: MySQL database config

```toml
[database]
name = "orders"
type = "sql"

[sql]
dialect = "mysql"
connectionString = "app:${MYSQL_PASSWORD}@tcp(db.internal:3306)/orders"
```

`${MYSQL_PASSWORD}` is resolved from the node's environment when the
config is parsed. The stored config keeps the literal
`${MYSQL_PASSWORD}` template.

## Operational rule: provision the env var on every node first

> **Set the secret on _every_ node — via a rolling restart — _before_ you
> propose a config that references it. A node that is missing a
> referenced `${VAR}` will crash or refuse to start; it does not skip the
> config and keep running.**

This is the single most important thing to understand about this
feature. Interpolation happens locally on each node, so a config that
references `${FOO}` is only safe to propose once **all** nodes already
have `FOO` in their environment. The correct order of operations is:

1. Add the variable to every node's environment and restart each node
   (rolling restart — environment is read at process start; there is no
   hot reload). For a secret rotation, this is also the procedure: update
   the env var and restart; you do **not** propose a new config.
2. Only then submit (or re-submit) the config that references it.

If you propose first, every node still missing the variable will fail as
described below.

## Failure mode: missing variable

A `${VAR}` whose variable is not set is a **hard error** — never a
silent empty credential, and never a silently-skipped config. There are
three points where it can surface, and all three are fatal by design:

- **At submit time**, on the node that receives the API call (it
  interpolates against its own environment before proposing): the request
  is rejected with **HTTP 400** naming the missing variable. Nothing is
  proposed.

- **At startup**, every persisted config is re-validated against the
  current environment before the node begins serving. A node missing a
  variable that a stored config references **fails to start**:

  ```
  cannot open storage: database config "orders": environment variable "MYSQL_PASSWORD" referenced in config is not set
  ```

- **At apply time**, if a config is proposed (and committed) while a
  *running* node is missing the variable, that node **crashes** when it
  applies the entry (a fatal apply error). This is not optional or
  configurable: applying committed entries must be deterministic across
  the cluster, so a node that cannot apply an entry every other node
  applied must stop rather than silently diverge. The crash is the
  cluster telling you the env-first ordering above was not followed on
  that node — fix its environment and restart it.

All three are deliberate. A node that booted or kept running with an
unresolved secret would otherwise fail much later with a confusing
authentication error against the external database; failing loudly and
early turns it into an obvious operator fix. (Only the missing-secret
case is treated this strictly. An otherwise malformed stored config is
handled by the existing, more lenient load paths.)

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
