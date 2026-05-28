# Use goreman to run a local 3-node cluster: `go get github.com/mattn/goreman` then `goreman start`.
#
# Configuration is via COMMITTED_* env vars (the binary has no positional/--flag config):
#   COMMITTED_NODE_ID  - this node's raft ID (must be unique, and present in COMMITTED_PEERS)
#   COMMITTED_API_ADDR - HTTP API listen address
#   COMMITTED_PEER_URL - this node's advertised raft peer URL
#   COMMITTED_DATA_DIR - per-node data directory (WAL/state/etc); must differ per node
#   COMMITTED_PEERS    - full static membership, id=url pairs, identical on every node
#
# Single node: COMMITTED_PEERS unset -> bootstraps a one-node cluster on COMMITTED_PEER_URL.

committed1: COMMITTED_NODE_ID=1 COMMITTED_API_ADDR=:12380 COMMITTED_PEER_URL=http://127.0.0.1:12379 COMMITTED_DATA_DIR=./data1 COMMITTED_PEERS=1=http://127.0.0.1:12379,2=http://127.0.0.1:22379,3=http://127.0.0.1:32379 ./committed node
committed2: COMMITTED_NODE_ID=2 COMMITTED_API_ADDR=:22380 COMMITTED_PEER_URL=http://127.0.0.1:22379 COMMITTED_DATA_DIR=./data2 COMMITTED_PEERS=1=http://127.0.0.1:12379,2=http://127.0.0.1:22379,3=http://127.0.0.1:32379 ./committed node
committed3: COMMITTED_NODE_ID=3 COMMITTED_API_ADDR=:32380 COMMITTED_PEER_URL=http://127.0.0.1:32379 COMMITTED_DATA_DIR=./data3 COMMITTED_PEERS=1=http://127.0.0.1:12379,2=http://127.0.0.1:22379,3=http://127.0.0.1:32379 ./committed node
