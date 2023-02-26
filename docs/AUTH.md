# Authentication.  
The auth cluster will have a key/value store backed by a raft cluster. Authentication will be checked based on hashed/salted keys. Once authentication is successful a JWT access token with will be issued.  

The access token will be short lived (5-15 minutes) and contain a claim that lists all logs the user has access to  

There are two permissions: write to log, and add syncable to log  

Since the database should only be accessed from the backend, we don't need refresh tokens. We can simply re-authenticate when needed.

Authorization can be done on any node based on the access in the JWT claim  

Tokens will have an iat claim which nodes can use to determine when they expire.  

# Questions:
* How do we know which cluster is the auth cluster?
* How do we pass that information to other nodes?

# Control Plane
The control plane will handle command and control activities
1. Auth
    * key/value store of users and user details like salted password hashes, permissions, etc.
2. Log Discovery
    * key/value store of fully qualified log names and cluster details like ip addresses, etc.
3. Cluster Health/Maintenance
    * Create new raft clusters
    * Move logs to different raft clusters
    * Raft cluster maintenance