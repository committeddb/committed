# Authenticating a Committed cluster

This runbook is for operators. It describes the two independent
authentication layers Committed ships today, how to turn each one on,
and how to keep the credentials healthy over time.

The layers protect different traffic and are configured
independently:

- **HTTP API bearer token** — guards the client-facing REST API on
  port 8080 (by default). Humans and scripts talking to Committed use
  this.
- **HTTP API TLS** — encrypts the client-facing REST API on the wire.
  Optional client-cert enforcement (mTLS) is available on top.
- **Peer mTLS** — guards the raft peer-to-peer transport between nodes
  in the same cluster. Nodes talking to each other use this.

You can mix these layers independently. Neither is acceptable for a
single-developer laptop; anything exposed beyond a trusted network
should have at least the bearer token **over TLS** (plaintext bearer
tokens travel in the clear), and production deployments should have
all three.

There is no authorization layer yet. "Authenticated = full access" is
the current model. A per-resource RBAC layer is filed as a follow-up
on the `http-authentication.md` ticket.

## HTTP API bearer token

### Enabling

Set one environment variable before starting each node:

```
COMMITTED_API_TOKEN=<long-random-string>
```

Any request to a protected route must then carry an `Authorization`
header:

```
Authorization: Bearer <long-random-string>
```

Unset or empty disables auth entirely. On startup the node logs a
warning in that case; do not run production traffic through a node in
this state.

### Generating a token

Any high-entropy opaque string works. Practical recipes:

```
# 32 bytes of hex — 64 characters, 256 bits of entropy
openssl rand -hex 32

# 32 bytes of URL-safe base64 — 43 characters, 256 bits
openssl rand -base64 32 | tr '+/' '-_' | tr -d '='
```

Either is fine. Record it somewhere it can be audited (a password
manager, a secrets store) — Committed itself does not re-emit the
token anywhere you can recover it from.

### Routes covered

The middleware is wired on every HTTP route except:

- `/health` — liveness probe, always anonymous
- `/ready` — readiness probe, always anonymous

Every other route (including `/metrics` if present, every `/database`,
`/proposal`, `/syncable`, `/ingestable`, `/type`, `/openapi.json`
endpoint) requires the bearer token. Responses without it return
`401 Unauthorized` with a structured error body.

### Distributing the token

Committed itself has no token-distribution mechanism — it expects the
environment to already be set when the process starts. Usual
approaches:

- **systemd**: set `Environment=COMMITTED_API_TOKEN=...` in the unit,
  with the unit file permissioned `0600`. Or use `EnvironmentFile=` to
  pull from a separately-permissioned file.
- **Kubernetes**: a `Secret` projected into the pod as an env var via
  `envFrom.secretRef` or `env[].valueFrom.secretKeyRef`.
- **Docker**: `--env-file` pointing at a file that is not itself
  committed to source control.

Never pass the token on the command line — it becomes visible in `ps`
output.

### Rotating the token

There is no hot-reload today. Rotation requires a restart:

1. Generate the new token.
2. Update the env var source (unit file, Secret, env file).
3. Restart each node in turn, giving the cluster time to converge
   between restarts.
4. Update any client configuration to use the new token.

If you need to rotate without downtime, run clients with both tokens
in rotation and flip them through a reverse proxy until migration
completes — Committed's single-token design doesn't support
simultaneous valid tokens natively.

## HTTP API TLS

### Why TLS for the client API

The REST API carries every configuration payload, every proposal
body, and — once the bearer token is enabled — the token itself on
every request. Without TLS all of this travels in the clear. Any host
on the path can read credentials and data, and any host with a
convenient MITM position can forge or modify requests. Bearer tokens
on plaintext HTTP are not a production security boundary.

### Enabling

Set the cert + key env vars on each node:

```
COMMITTED_HTTP_TLS_CERT_FILE=/etc/committed/api.pem
COMMITTED_HTTP_TLS_KEY_FILE=/etc/committed/api.key
```

Both must be set together. Setting one without the other is a hard
startup error — same rationale as peer mTLS below: silent fallback
to plaintext when operators think they have TLS is the failure mode
this check exists to prevent.

Neither set keeps today's plaintext behaviour with a startup warning
log. Acceptable for local dev; anything else should have TLS on.

The TLS 1.2 floor is enforced by the server — older TLS 1.0/1.1
clients are rejected at handshake time.

### Optional client-cert auth (mTLS)

To require a client certificate in addition to (or instead of) a
bearer token, set a third env var:

```
COMMITTED_HTTP_TLS_CLIENT_CA_FILE=/etc/committed/client-ca.pem
```

With that set, the server requires every client to present a cert
signed by the CA in the named file. A stolen bearer token is useless
without a client cert that chains to this CA — strictly more secure
than bearer alone, at the cost of issuing and distributing client
certs.

This is orthogonal to the peer-mTLS CA; use the same CA if you want
one PKI, or a separate CA for API clients if you want them scoped
independently.

### Generating a cert

Any PKI works. For the cheapest self-contained setup, reuse the
openssl recipe from the peer-mTLS section below — generate a CA,
then issue a server cert whose SAN covers the hostname operators and
scripts will dial (e.g. `api.example.com`, or `127.0.0.1` for a
laptop).

Permissions:

```
chown committed:committed api.pem api.key
chmod 0644 api.pem
chmod 0600 api.key
```

### What's not in scope

- **Hot-reload of API certs**: rotating the cert requires a restart,
  same as peer mTLS.
- **ACME / Let's Encrypt integration**: out of scope; run a TLS
  terminator (nginx, Caddy, a cloud LB) in front if you want this.
- **Plaintext-to-HTTPS redirect on the same process**: also out of
  scope. If you need both ports, terminate TLS in front.

## Peer mTLS

### Why mTLS for peers

The raft transport between nodes carries every proposal, including
entity payloads, conf-change messages, and cluster metadata. Without
TLS it is plaintext and any host with network access to the raft port
can impersonate a peer (forge leader elections, inject log entries,
eavesdrop on data). Bearer tokens are the wrong shape for peer traffic
— it's long-lived process-to-process, not request-response — so the
standard fix is mTLS: both sides present certs signed by a
cluster-wide CA before any application bytes flow.

### Enabling

Set all three environment variables on each node:

```
COMMITTED_TLS_CA_FILE=/etc/committed/ca.pem
COMMITTED_TLS_CERT_FILE=/etc/committed/node.pem
COMMITTED_TLS_KEY_FILE=/etc/committed/node.key
```

All three must be set together. Any other combination (one set, two
set) is a hard startup error — the node refuses to boot. That's
deliberate: silent fallback to plaintext when operators think they
have TLS is a worse failure mode than a noisy startup refusal.

**Peer URLs must use `https://`** when mTLS is enabled. Mixed schemes
(some nodes configured for TLS, others for plaintext, or URLs that
disagree with the TLS config) manifest as TLS handshake errors at
connect time. Fix the URLs so every peer's `--url` points at the
`https://` variant.

### Generating a cluster CA

Any PKI will work as long as every node's cert chains to the same
root. The simplest self-contained option is `openssl`:

```
# 1. Generate the CA private key and self-signed root cert.
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes \
  -key ca.key \
  -sha256 -days 3650 \
  -subj "/CN=committed-cluster-ca" \
  -out ca.pem
```

That root cert (`ca.pem`) is what every node gets as its
`COMMITTED_TLS_CA_FILE`. The root private key (`ca.key`) is the crown
jewel — anyone who has it can mint a cert the cluster will trust. It
should live somewhere safe (a dedicated laptop kept offline, a
hardware security module, Vault's PKI engine) and should NOT live on
any cluster node.

For larger deployments, `cfssl` or `step-ca` give you the same
cryptography with better ergonomics around CSRs, automation, and
rotation. HashiCorp Vault's PKI secrets engine covers the whole
lifecycle if you already run Vault.

### Issuing a node cert

For each node, generate a key and a CSR, then sign the CSR with the
CA. Committed expects the cert to be valid for both server and client
roles — peer connections flow in both directions.

```
# For node with hostname node-1.example.com:

# 1. Generate this node's private key.
openssl genrsa -out node.key 2048

# 2. Generate a CSR. Include the DNS name in the SAN so peer
#    verification succeeds when peers dial https://node-1.example.com.
cat > node.conf <<EOF
[req]
distinguished_name = req
req_extensions = v3_req
prompt = no
[req]
CN = node-1.example.com
[v3_req]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth, clientAuth
[alt_names]
DNS.1 = node-1.example.com
IP.1 = 10.0.0.1
EOF

openssl req -new -key node.key -out node.csr -config node.conf

# 3. Sign the CSR with the CA. Carry the SAN and extended key usage
#    through from the CSR's v3_req extensions.
openssl x509 -req -in node.csr \
  -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out node.pem -days 365 -sha256 \
  -extfile node.conf -extensions v3_req
```

Copy `ca.pem`, `node.pem`, and `node.key` to each node at the paths
configured in the env vars. File permissions:

```
chown committed:committed ca.pem node.pem node.key
chmod 0644 ca.pem node.pem      # public
chmod 0600 node.key             # secret
```

The CA root itself (`ca.pem`) is not secret — every node needs it to
verify peers. The node's private key (`node.key`) IS secret — lock it
down.

### Cert lifecycle

Certs expire. Committed does not hot-reload them today, so every
rotation is a rolling restart.

- **Typical lifetime**: node certs for 365 days, CA root for 10
  years. Pick whatever matches your org's policy; the commands above
  encode these as defaults.
- **Monitoring expiry**: run `openssl x509 -in node.pem -noout
  -enddate` in your monitoring to alert well before a cert expires. A
  cert that expires mid-flight manufactures an outage by taking the
  affected node off the cluster.
- **Rotating a node cert**: issue the new cert from the existing CA,
  copy it to the node, restart the node. The rest of the cluster
  continues to trust it because the CA root didn't change.
- **Rotating the CA root**: harder — you need an overlap window where
  both old and new CA roots are trusted by every node. For a small
  cluster, the honest approach is scheduled downtime and a
  re-issuance of every node cert at once. For a larger cluster, use a
  tool like `step-ca` that supports CA rotation first-class.
- **Revocation**: not supported by the current transport. The
  workaround is short-lived certs (90 days or less) and re-issue on
  schedule; a compromised cert expires on its own within the window.
  If you need immediate revocation before the cert would have expired,
  rotate the CA — every cert signed by the old CA is now untrusted.

### Verifying mTLS is on

After starting the cluster with TLS configured, confirm the transport
is refusing plaintext:

```
# From any host that can reach the raft port. Should hang or RST —
# NOT return a 200 with a raft handshake.
curl -v http://node-1.example.com:9022/raft

# With a curl that can carry a client cert, succeeds:
curl -v https://node-1.example.com:9022/raft \
  --cacert /etc/committed/ca.pem \
  --cert /etc/committed/node.pem \
  --key /etc/committed/node.key
```

Node logs at handshake time also surface rejected peers — lines like
`tls: client didn't provide a certificate` or `certificate signed by
unknown authority` mean the listener is correctly turning away
unauthorized clients.

## Choosing what to run

- **Laptop / local dev**: nothing. `goreman start` with no auth config
  is the expected development shape.
- **Shared dev / staging on a trusted LAN**: bearer token + API TLS at
  minimum; peer mTLS if the cluster spans machines.
- **Production, multi-tenant networks, anything internet-reachable**:
  all three — bearer token, API TLS (client-cert mTLS if you can
  issue them), and peer mTLS.

The layers are independent by design. Turning one on doesn't force
the others. The bearer token and API TLS guard the API you hand to
humans; peer mTLS guards the traffic between nodes. They fail loudly
in different directions — a missing bearer token returns a
structured `401`; a failed TLS handshake rejects the connection
entirely and logs on the server side — so operational debugging
rarely needs to distinguish them.
