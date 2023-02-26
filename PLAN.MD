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