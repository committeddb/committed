# Committed

[![CI](https://github.com/philborlin/committed/actions/workflows/ci.yml/badge.svg)](https://github.com/philborlin/committed/actions/workflows/ci.yml)

A single-binary, Raft-backed CDC pipeline with the log as its own source of truth.

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use adddons, or 3rd party software to aid in your read activites, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a databse designed for querying.

## What makes it distinctive

- **vs. Kafka**: a single replicated log instead of partitioned topics, with built-in *output-side* projection (syncables) rather than consumer offsets. Configs live in the log, not ZooKeeper/KRaft.
- **vs. etcd**: same Raft substrate, but append-only log semantics instead of KV — and a worker model for ingest/sync that etcd doesn't have.
- **vs. an RDBMS / Debezium pipeline**: Committed collapses "replicated log + CDC source + sink connectors" into one process. You don't need Kafka + Debezium + Kafka Connect + a separate consensus layer; the same binary holds the log, the source, and the sink.

## Version 0.2
This is a beta version of Committed. You can spin up a cluster. See the included Procfile for an example of how to run the server, run the Procfile directly using goreman or something similar that can execute Procfiles, or checkout our
[sandbox](http://www.committeddb.com/sandbox).

Once the server is running you should see 3 leader elections (if you change the number of nodes you should see every node come to consensus). When the server is working your terminal should look something like this:

```
08:13:19 committed1 | raft2018/04/30 08:13:19 INFO: raft.node: 1 elected leader 1 at term 2  
08:13:19 committed2 | raft2018/04/30 08:13:19 INFO: raft.node: 2 elected leader 1 at term 2  
08:13:19 committed3 | raft2018/04/30 08:13:19 INFO: raft.node: 3 elected leader 1 at term 2  
```

The term number doesn't matter and who got elected leader doesn't matter either.

Once started committed will create two folders per node. The first node will be called raft-node# and raft-node#-snap. These folders contain all of your data. To clear the data in the server you can simply delete the folders.

The beta has four things you can do:

* Add a topic
* Append to a topic
* Add a database
* Add a SQL syncable

To Add a topic POST to http://server:port/cluster/topics with an HTTP body that looks like:  
```
[topic]
name = "test1"
```

To Append to a topic POST to http://server:port/cluster/posts with an HTTP body that looks like:  
```
{  
	"Topic" : "test1",  
	"Proposal" : {
		Key: "baz", One: "qux"
	}
}
```

To add a database POST to http://server:port/cluster/databases with an HTTP body that looks like:
```
[database]
name = "testdb"
type = "sql"

[sql]
dialect = "mysql"
connectionString = "myConnectionString"
```

The Database is a TOML configuration file. Currently the only type supported is sql and the only dialect supported is mysql.

To Add a syncable POST to http://server:port/cluster/syncables with an HTTP body that looks like:  
```
[syncable]
name = "foo"
# Determines what the rest of the config will look like
type = "sql"

[sql]
topic = "test1"
db = "testdb"
table = "foo"
primaryKey = "pk"

[[sql.indexes]]
name = "firstIndex"
index = "one"

[[sql.mappings]]
jsonPath = "$.Key"
column = "pk"
type = "TEXT"

[[sql.mappings]]
jsonPath = "$.One"
column = "one"
type = "TEXT"
```

The Syncable is a TOML configuration file.

The workflow would look like: Add a topic, Append some proposals, Add a database, Add a syncable, look at your SQL database and see the proposals get applied to the DB, Add some more proposals and then read the database and do something useful.

## Road Map

Next up is version 0.3.

Although there are no promises about what will be in the next release, we are currently working on a topic
syncable that allows you to transform data from one topic and write the transformed data to a new topic

## Sandbox

There is an online sandbox that allows you to play with a shared server without having to download or install the database yourself. You can find more information at [http://www.committeddb.com/sandbox](http://www.committeddb.com/sandbox)

## Operations

### Storage layout

A Committed node's data directory looks like this:

```
<datadir>/
├── raft/
│   ├── log/         # raft consensus log; bounded (10GB / 1hr)
│   └── state/       # HardState + ConfState
├── events/          # permanent event log; infinite retention
├── metadata/
│   └── bbolt.db     # types, databases, syncables, applied index
└── time-series/     # derived view regenerable from events/
```

The permanent event log under `events/` is the canonical record of
every write; syncables read from it, and it is never compacted.
`raft/log/` is consensus transport only and is compacted aggressively.
See [`docs/event-log-architecture.md`](docs/event-log-architecture.md)
for the rationale.

### Rebuilding a node

If a node logs a `storage invariant violation` fatal message, it has
fallen too far behind the cluster to recover via normal raft
replication. The runbook for this case —
[`docs/operations/rebuild.md`](docs/operations/rebuild.md) — covers
both existing-node rebuild and adding a new node. The short version
is: stop the node, rsync its data directory from a healthy peer,
restart. Apply determinism keeps subsequent rebuilds O(diff).
