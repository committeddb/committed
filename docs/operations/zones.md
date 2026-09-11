# Zones: pinning sync egress to the data's neighborhood

A cluster spanning zones (availability zones, racks, sites) pays cross-zone
traffic for raft replication once per follower — that is the durability
product, and it is unavoidable. But **sync egress** from the leader to a destination
in another zone pays a second, redundant crossing: the bytes already live on
the node sitting next to that destination, in its own event log. With a read-model
database per zone, leader-based sync pays cross-zone for most destinations on every
entity, forever.

Zone-pinned syncables drop that to zero:

1. Give each node a zone identity: `COMMITTED_ZONE=us-east-1c` (env-only,
   like all node config). The node announces it into the cluster at startup.
   Vendor-neutral — a zone can be an AZ, a rack, or a site.
2. Pin a syncable to its destination's zone:

   ```toml
   [syncable]
   name = "reader-east-1c"
   type = "projection"
   zone = "us-east-1c"
   # … the rest of the config is unchanged
   ```

The syncable is then served by the node in that zone (lowest node ID when a
zone has several), reading from its **local** event log — same data, same
order, same replicated checkpoint, zero extra crossings. A leader failover
does not move it. Without `zone`, behavior is exactly today's: the leader
serves.

## Strict pins: stall, never fall back

Ownership is a pure function of replicated state: the lowest-numbered
current **member** announcing the pinned zone owns the syncable. Liveness
plays no part, on purpose — a resolution that depended on who each node
believes is alive could name two owners at once, which is the one thing a
pin must never do. So a pinned syncable stalls in two shapes, and no other
node ever takes over in either:

- **No member in the zone** (the zone's last node was removed from the
  cluster). The pin is unsatisfiable: `pinUnsatisfiable: true` and
  `ownerNode: 0` on `GET /v1/syncable/{id}/status`.
- **The owner is down but still a member** (crashed, partitioned, powered
  off). The pin still resolves to it: `ownerNode` names the dead node and
  `pinUnsatisfiable` stays `false`. The status endpoint answers from any
  node, and this is the shape it cannot flag by itself: what you see is
  `lag` growing with no `stuck` and no park, and `GET /v1/membership`
  showing `active: false` for that node.

Both are deliberate: a silent leader fallback would quietly reintroduce the
cross-zone cost the pin exists to avoid, and hide the topology problem. The
event log is permanent, so a stalled syncable always catches up completely
when the owner returns: **lag, never loss**.

**Alert on lag that stops shrinking**, not only on `pinUnsatisfiable`; a
pinned syncable's lag is the one signal both shapes share. When the owner
is gone for good, `committed member remove` it: ownership moves to the next
member announcing the zone, or the pin becomes unsatisfiable and says so.

## Admission and upgrades

- A `zone` matching no announced current member is refused at POST (400) —
  set `COMMITTED_ZONE` on the node first.
- On a **mixed-version cluster** (rolling upgrade from below 0.8.0), pinned
  configs are refused with 503 `cluster_below_feature_level` until every
  member is upgraded, and every node resolves leader-owns until then. This
  is what guarantees a pin can never produce two concurrent writers to one
  destination mid-upgrade.
- The `rebuild` and `rematerialize` verbs work on pinned syncables and are
  ROUTED to the serving node automatically (one bounded hop — set
  `COMMITTED_API_URL` on every node, as for follower proxying generally):
  their worker-drain step must run where the worker runs. While a pin is
  unsatisfiable the verbs answer 503 `pin_unsatisfiable` (there is no
  worker anywhere to drain); restore the zone and retry.

## What it saves, and what it doesn't

- **Saved**: the sync-egress crossing for every same-zone destination — typically
  the dominant recurring volume (every entity, to every read model,
  forever).
- **Still paid**: raft replication to each follower (the durability
  product), and ingress — client proposals still route to the leader.
  Ingest is likewise leader-based today; pinning it is a natural follow-on
  if the need appears.
