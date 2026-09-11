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

## Strict pins: what happens when the owner dies

Nothing happens to the syncable, and that is by design.

**What the cluster does.** Ownership is decided from cluster membership:
the lowest-numbered current member announcing the zone owns the pin.
Whether a node is alive never enters that decision, because a decision
based on liveness could name two owners at once, and two nodes writing to
one destination is the one thing a pin must never allow. So a dead node
that is still a member keeps the pin. No other node takes over, and the
leader does not step in, since that fallback would silently reintroduce
the cross-zone cost the pin exists to avoid.

**What you see.** The syncable's `lag` grows and nothing else changes.
`GET /v1/syncable/{id}/status` shows no park and no `stuck`, because there
is no worker anywhere to report either. `ownerNode` still names the dead
node, and `pinUnsatisfiable` stays `false`: that flag means "no member
announces this zone", which is a different situation (the zone's last node
was removed), and shows as `pinUnsatisfiable: true` with `ownerNode: 0`.
`GET /v1/membership` shows the dead node as `active: false`. **Alert on
lag that stops shrinking**; it is the one signal both situations share.

**What is at risk.** Nothing is lost. The event log is permanent and the
checkpoint is replicated, so when the node returns the syncable resumes
where it left off and catches up completely: lag, never loss. The cost is
latency on that one syncable for as long as the node is away.

**What you can do.** Wait for the node to return, or, if it is gone for
good, remove it with `committed member remove`. Ownership then moves to
the next member announcing the zone, or, if there is none, the pin becomes
unsatisfiable and says so on status.

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
