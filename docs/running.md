# Running Committed

How to get the binary or container, start a single node or a three-node cluster, and configure a node.

## Starting a node

Download a prebuilt binary for your platform from the
[releases page](https://github.com/committeddb/committed/releases), or
build from source:

```sh
make build
```

Single node — defaults to ID=1, HTTP at `:8080`, data dir `./data`:

```sh
./committed node
```

Or run the release container (distroless, static, runs as nonroot uid
65532). It reads the same `COMMITTED_*` env vars and persists WAL/state
under `/home/nonroot/data`:

```sh
docker run --rm -p 8080:8080 -p 9022:9022 \
  -v committed-data:/home/nonroot/data \
  committeddb/committed:0.8.1
```

`docker run committeddb/committed:0.8.1 --version` prints the build
identity; `:latest` tracks the most recent release. See
[Configuration](#configuration) for the env vars and [`docker-compose.yml`](../docker-compose.yml)
for a local single-node setup.

Three-node cluster via [goreman](https://github.com/mattn/goreman) and
the included `Procfile`:

```sh
goreman start
```

You'll see leader-election logs on each node. The current leader serves
HTTP writes; followers forward proposals over Raft.

## Configuration

A node is configured entirely through environment variables, so the
same image can be templated per-node by an orchestrator:

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMMITTED_NODE_ID` | `1` | Raft node ID. Must be unique and appear in `COMMITTED_PEERS`. |
| `COMMITTED_API_ADDR` | `:8080` | HTTP API listen address. |
| `COMMITTED_DATA_DIR` | `./data` | Directory for the WAL, raft state, and metadata. |
| `COMMITTED_PEER_URL` | `http://127.0.0.1:9022` | This node's advertised raft peer URL. Used when `COMMITTED_PEERS` is unset. |
| `COMMITTED_PEERS` | _(unset)_ | Full static cluster membership as `id=url` pairs, e.g. `1=http://n1:9022,2=http://n2:9022,3=http://n3:9022`. Give the same value to every node; it must include this node's `COMMITTED_NODE_ID`. |
| `COMMITTED_ZONE` | _(unset)_ | This node's zone (vendor-neutral: an AZ, rack, or site), announced into the cluster at startup. A syncable config with `zone = "..."` is served by a node in that zone instead of the leader — see [`operations/zones.md`](operations/zones.md). |
| `COMMITTED_API_URL` | _(unset)_ | This node's advertised HTTP API base URL (e.g. `http://n1:8080`), self-announced into the cluster so followers can proxy leader-only reads. Set it on every node. |

`COMMITTED_PEERS` is consumed only on a node's **first** boot
(`raft.StartNode`). After that, membership is restored from the WAL on
restart, so editing `COMMITTED_PEERS` has no effect — use the
membership API for live changes (see
[`operations/membership.md`](operations/membership.md)). When
`COMMITTED_PEERS` is unset the node bootstraps a single-node cluster
advertising `COMMITTED_PEER_URL`.

Additional operational variables (peer/API mTLS, proposal-size and HTTP
timeout limits, graceful-shutdown deadline, OTel export) are documented
under [`operations/`](operations/).
