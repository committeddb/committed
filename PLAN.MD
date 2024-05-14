# Control Plane
## User Actions
1. Add User
1. Add Organization (if a user creates an organization they are automatically added to it)
1. Add user to Organization
1. Add API key to Organization
1. Add Log to Organization (uses maintenance action Add Raft to Multi-Raft Cluster)
1. Add syncable to Log
1. Add datastore connection information (including credentials) so they can be referenced in a syncable
1. Write to Log
1. Delete Data from Log (right to be forgotten)
## Maintenance Actions
1. Add Node
1. Add Multi-Raft Cluster to Nodes
1. Add Raft to Multi-Raft Cluster
## Learnings
1. Have a single raft for control plane data
1. Use a syncable to sync to a key/value store (eat our own dog food)
1. The leader can be found in the NodeHost.GetLeaderID(shardID)
1. The log can be read by getting a ReadonlyLogReader by calling NodeHost.GetLogReader(shardID)
1. Proposals can be made with an INodeUser by calling NodeHost.GetNodeUser(shardID)
## Right to be forgotten
1. Add an entity (or correlation) id to each proposal
1. Add a forget command that takes an entity (or correlation) id and deletes all proposals with that entity (or correlation) id from the log
## Single Region - Multi AZ
1. Syncables write to a single data store (such as a single RDS cluster or single Redis cluster)
1. 3 node cluster with one node in each AZ
## Multi Region
1. In Multi Region syncables write to a separate data store per region (such as a single RDS cluster or single Redis cluster) in order to support Multi Region architectures
1. 5 node cluster deploys in 3 Regions (a, b, c) with 2 hosts in region a, 2 hosts in region b, and 1 host in region c. Region a and b are hot and region c is standby because regions a and b can be a leader with one host and serve syncables with another but region c can't
1. 7 node cluster deploys in 3 Regions (a, b, c) with 3 hosts in region a, 2 hosts in region b, and 2 host in region c. All three Regions are hot.