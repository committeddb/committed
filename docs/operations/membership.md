# Changing cluster membership

This runbook is for operators adding or removing nodes from a running
Committed cluster. Membership changes use Raft **joint consensus**
(etcd/raft `ConfChangeV2`), which makes them safe under partition: no
add or remove can lose a committed entry or split the cluster, regardless
of timing or failures during the change.

Background on the consensus layer lives in
[`docs/event-log-architecture.md`](../event-log-architecture.md). The
first-boot peer set is described in the `node` command help
(`COMMITTED_PEERS`).

## How it works

A configuration change does not jump straight from the old member set to
the new one. It transits an intermediate **joint configuration** that is
the union of both:

```
C_old   →   C_old ∪ C_new (joint)   →   C_new
```

While joint, every decision — leader elections and commit
acknowledgements alike — requires a majority of **both** `C_old` and
`C_new` at the same time. Because both quorums must independently agree
throughout the transition, two disjoint majorities can never form, so no
committed entry can be lost. This is strictly safer than the single-step
(`AddNode`/`RemoveNode`) protocol, which is only safe when consecutive
configurations are guaranteed to overlap.

Committed uses the **implicit** joint transition
(`ConfChangeTransitionJointImplicit`): the operator submits one change to
enter the joint configuration, and once it commits, the leader
automatically proposes the second change to leave it. There is no second
step for the operator to run, and no half-finished state to recover after
a crash — a node that restarts mid-transition is still in a safe joint
configuration and the leader completes the leave.

A membership change is **partition-safe** and **node-agnostic**: submit
it to any member (a follower forwards the proposal to the leader). The
API call blocks until the answering node observes the change in the final
(non-joint) configuration, so a `204 No Content` (or a successful
`member` command) means the cluster has applied the new membership.

## Adding a node

Adding a node is a two-step operator workflow, because Raft replicates
only node **ids**, not their network addresses — the new node has to be
reachable before the cluster can talk to it.

### Step 1 — start the new node in join mode

Start the new node with `COMMITTED_JOIN=true`. In join mode the node
comes up with **no** Raft configuration and learns its membership from
the leader, instead of bootstrapping a new cluster from its static peer
set. Set `COMMITTED_PEERS` to the full membership **including the new
node itself** — it is used only to seed the peer transport (so the node
can reach the existing members and bind its own listener), not to
bootstrap a configuration.

```
COMMITTED_NODE_ID=4
COMMITTED_JOIN=true
COMMITTED_PEER_URL=http://n4:9022
COMMITTED_API_URL=http://n4:8080   # advertised API address (see "Observing membership")
COMMITTED_PEERS=1=http://n1:9022,2=http://n2:9022,3=http://n3:9022,4=http://n4:9022
COMMITTED_DATA_DIR=/var/lib/committed   # a fresh, empty data dir
```

The node will log `joining cluster` and then wait — it is not yet a
member and cannot serve traffic.

### Step 2 — add it to the cluster

Run `member add` against any existing node, naming the new node's id and
its advertised peer URL (the same value as its `COMMITTED_PEER_URL`):

```
committed member add --id 4 --url http://n4:9022 --target http://n1:8080
```

The existing cluster proposes the joint-consensus add; every member
learns node 4's address, the leader replicates the log (or a snapshot) to
it, and node 4 becomes a voter. The command returns once the change has
taken effect.

## Removing a node

```
committed member remove --id 4 --target http://n1:8080
```

The removed node steps out of the configuration once the change commits
and stops participating. Stop its process afterward; it will not rejoin
unless explicitly added again. Removing the node that is *serving the
request* is allowed — it finalizes the change and then leaves.

> **Quorum:** a removal that would drop the cluster below quorum cannot
> commit. Removing one node from a three-node cluster leaves two, which
> still requires both to be healthy to commit — plan capacity
> accordingly.

## Observing membership

`GET /v1/membership` lists the cluster: each member's role and the
leader-observed **matched index** (how far that member has replicated),
alongside the snapshot's leader, term, commit, and applied indices.

```json
{
  "node_id": 1, "leader_id": 1, "term": 5,
  "commit_index": 1234, "applied_index": 1234, "is_leader": true,
  "members": [
    { "id": 1, "role": "voter", "match_index": 1234, "api_url": "http://n1:8080" },
    { "id": 2, "role": "voter", "match_index": 1230, "api_url": "http://n2:8080" }
  ]
}
```

The per-member `match_index` is **leader-only** state in raft, so the read
is always answered by the leader: a request that lands on a follower is
transparently **proxied to the leader**. This is what lets a caller behind
a load balancer (a single VIP, no per-node addressing) get a
leader-truthful answer from *any* node.

For the proxy to reach the leader, each node advertises its HTTP API base
URL via **`COMMITTED_API_URL`** (e.g. `http://n1:8080`) — the API-plane
sibling of `COMMITTED_PEER_URL`. Each node self-announces its own URL into
the cluster on startup, so set it on **every** node (bootstrap and joined
alike). It survives restarts and snapshots; you only ever set each node's
own.

If the leader has not announced an API URL (`COMMITTED_API_URL` unset), or
no leader is currently known, a follower can't proxy and returns **503**
with the believed leader id in the error details (`{"details":{"leader_id":
N}}`) — target node `N` directly, or retry.

> **Catch-up is the caller's call.** The server reports `match_index` and
> `commit_index` but does not judge "caught up" — a caller compares them
> with its own threshold. (This is the observability the learner-promotion
> workflow builds on.)

## CLI reference

```
committed member add    --id <n> --url <peer-url>   [flags]
committed member remove --id <n>                    [flags]
```

Common flags:

| Flag         | Default                       | Meaning                                                       |
|--------------|-------------------------------|--------------------------------------------------------------|
| `--target`   | local `COMMITTED_API_ADDR`    | base URL of the cluster node's API to drive the change       |
| `--token`    | `COMMITTED_API_TOKEN`         | bearer token for the authenticated API                       |
| `--insecure` | off                           | skip TLS verification when `--target` is `https://`          |

With no `--target`, the command talks to the local node's API (deriving
`https://` automatically when `COMMITTED_HTTP_TLS_CERT_FILE` is set, the
same way the `healthcheck` probe does).

## HTTP API

Membership lives under `/v1` (all authenticated):

```
GET    /v1/membership            → membership + replication progress (see "Observing membership")
POST   /v1/membership            {"id": 4, "url": "http://n4:9022"}
DELETE /v1/membership/{id}
```

The `member add`/`remove` CLI commands are thin wrappers over `POST`/`DELETE`;
the `GET` is consumed directly over HTTP (by an orchestrator's scheduler, or
ad hoc with `curl`).

`POST`/`DELETE` return `204 No Content` on success. Error responses use the
standard structured JSON body (`{"code", "message"}`):

| Status | When                                                                       |
|--------|----------------------------------------------------------------------------|
| `400`  | malformed request — zero/missing id, empty url on add, non-numeric id      |
| `503`  | a write: submitted but not confirmed before the deadline (this node likely cannot reach a quorum); may still take effect once quorum returns. A `GET`: no known leader, or the leader's API address is unknown — retry or target the leader directly |
| `500`  | unexpected internal error                                                  |

## Notes

- **Restart vs. membership.** `COMMITTED_PEERS` is consumed only on first
  boot. After a node has state, its membership is restored from the WAL,
  so editing `COMMITTED_PEERS` has no effect — use these commands for any
  live change.
- **Backward compatibility.** A node upgraded from a pre-joint-consensus
  build correctly replays any single-step (`ConfChange` v1) entries left
  in its log; the cluster proposes only v2 entries going forward.
- **Learners.** Promoting learners to voters is not yet exposed. Adds
  create voting members directly.
