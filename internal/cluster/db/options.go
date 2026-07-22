package db

import (
	nethttp "net/http"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/metrics"
)

// defaultTickInterval is the cadence at which Raft.Tick() is called when no
// override is supplied. It interacts with the (currently hard-coded)
// ElectionTick=10 and HeartbeatTick=1 settings: with a 30ms tick, leader
// election fires after ~300ms and heartbeats fire every 30ms. The 300ms
// election timeout is the upper end of Raft paper §5.2's recommended
// 150-300ms range and is conservative enough to absorb routine GC pauses
// and modest network jitter on real-world deployments.
//
// Operators who run on a low-latency LAN and want faster failover can
// drop this to 15ms via WithTickInterval (yields a 150ms election
// timeout — the lower end of the paper's range). Going below 15ms is
// strongly discouraged in production: GC pauses or transient packet
// loss will trigger spurious elections.
//
// Tests override this with their own (much faster) values via
// WithTickInterval — see testTickInterval and multiNodeTickInterval.
const defaultTickInterval = 30 * time.Millisecond

// Option configures behaviour of New. Options exist primarily to let tests
// shorten timing-sensitive constants; production callers should pass none.
type Option func(*options)

type options struct {
	tickInterval time.Duration
	// announceVersion enables the startup feature-level self-announce
	// (db.announceVersion), which proposes this node's version.FeatureLevel so
	// the version-skew gate can compute the cluster-agreed minimum. Production
	// enables it via WithVersionAnnounce; it defaults OFF so the bulk of unit
	// tests — which build single-node DBs and assert exact committed-entry
	// counts — aren't perturbed by the extra background proposal (and its commit
	// racing their assertions). A node that never announces reads as level 0,
	// which only ever holds emission (the conservative, safe direction).
	announceVersion bool
	// leaderChangeGrace is how long db.DB's leader-change watcher waits
	// after observing a raft leader transition before signaling at-risk
	// waiters with ErrProposalUnknown. The grace period gives the
	// normal apply path a chance to win when the new leader inherits
	// and commits the entry fast — small enough to beat the caller's
	// ctx deadline, large enough that a smooth hand-off doesn't produce
	// spurious ErrProposalUnknowns. 0 means "derive from tickInterval
	// at New time" — see defaultLeaderChangeGracePeriod and db.New.
	leaderChangeGrace time.Duration
	logger            *zap.Logger
	metrics           *metrics.Metrics
	// compactMaxSize is the on-disk raft log size in bytes that, once
	// exceeded, triggers a CreateSnapshot + Compact at the end of the
	// current Ready iteration. 0 disables the size limb of the
	// compaction policy. See docs/event-log-architecture.md §
	// "Compaction policy".
	compactMaxSize uint64
	// compactMaxAge is the wall-clock time-since-last-compaction that,
	// once exceeded, triggers compaction regardless of raft log size.
	// Works in OR with compactMaxSize: whichever fires first drives
	// the trigger. 0 disables the age limb.
	compactMaxAge time.Duration
	// transportWrapper, if non-nil, is applied to the Transport that
	// startRaft constructs via httptransport.New, and the returned value
	// is used in place of the original. Test-only hook set via
	// WithTransportWrapperForTest (export_test.go) so the adversarial
	// suite can inject fault-injection wrappers around the real transport
	// without forking a parallel constructor path. Production callers
	// leave this nil and get the plain HttpTransport.
	transportWrapper func(Transport) Transport
	// tlsInfo, when non-nil, enables mTLS on the raft peer transport.
	// Populated from WithTLSInfo by cmd/node.go when the
	// COMMITTED_TLS_CA_FILE / CERT_FILE / KEY_FILE env vars are all set.
	// nil (the default) leaves the peer transport as plaintext HTTP.
	tlsInfo *transport.TLSInfo

	// apiToken is the cluster bearer token the peer transport sends on its
	// requests, injected from WithAPIToken by cmd/node.go (which reads
	// COMMITTED_API_TOKEN at the composition root). Empty leaves peer requests
	// unauthenticated. Injecting it keeps the env read out of the transport
	// constructor, where it was ambient global state.
	apiToken string

	// transportFactory builds the peer Transport. The composition root injects
	// it via WithTransportFactory (cmd wires the HTTP transport) so db never
	// imports a concrete transport. nil is a wiring error — startRaft panics
	// rather than run a node that can't reach its peers. It defaults to
	// pkgDefaultTransportFactory, which production leaves nil but the db test
	// suite sets once (see SetTestTransportFactory) so its many db.New/NewRaft
	// call sites don't each have to wire a transport.
	transportFactory TransportFactory

	// join marks this node as joining an existing cluster rather than
	// bootstrapping a new one. When true, startRaft calls raft.RestartNode
	// with an empty initial state instead of raft.StartNode(c, peers): the
	// node starts with no configuration and learns its membership from the
	// leader once an AddNode conf change naming it commits. The peer set is
	// still used to seed the transport (so the node can reach the existing
	// members and bind its own listener), but it does NOT become a
	// bootstrap configuration — bootstrapping from it would form a
	// competing single-node cluster. Set via WithJoin from cmd/node.go when
	// COMMITTED_JOIN is truthy. See docs/operations/membership.md.
	join bool

	// ingestSupervisor{InitialBackoff,MaxBackoff,MaxAttempts} govern the
	// auto-restart behavior when an ingest worker parks in a freeze branch. The
	// supervisor waits backoff (starting at InitialBackoff, doubling on each
	// successive freeze at the SAME resume position, capped at MaxBackoff)
	// before re-registering the ingestable via db.Ingest. After MaxAttempts
	// consecutive freezes at the same position the supervisor gives up and emits
	// IngestSupervisorGiveup; a freeze at an advanced position (real progress)
	// resets the run. Zero values mean "use package defaults" (see default*
	// constants in ingest_supervisor.go).
	ingestSupervisorInitialBackoff time.Duration
	ingestSupervisorMaxBackoff     time.Duration
	ingestSupervisorMaxAttempts    int

	maxProposalBytes uint64

	// syncStuckThreshold is how long a sync worker must be continuously
	// blocked retrying a transient error before it publishes a replicated
	// SyncableStuck record (so the stall is visible cluster-wide and an
	// operator can skip it). 0 means "use defaultSyncStuckThreshold". Tests
	// shorten it via WithSyncStuckThreshold; production leaves the default.
	syncStuckThreshold time.Duration

	// scrubInterval is the cadence at which the leader proposes a Scrub
	// command when there is unscrubbed RTBF backlog (delete proposals whose
	// PII originals are still in the permanent event log). The interval
	// amortizes the O(N) rewrite over batches of deletes and bounds the
	// erasure SLA. 0 disables the automatic scheduler entirely (the manual
	// POST /v1/scrub lever still works). See docs/event-log-architecture.md
	// § "Right-to-be-forgotten / deletes". Tests shorten it via
	// WithScrubInterval; production leaves the default.
	scrubInterval time.Duration

	// advertisedAPIURL is this node's advertised HTTP API base URL (e.g.
	// http://n1:8080), self-announced into the replicated memberAPIURLs map
	// on startup so other nodes can resolve this node's API address to proxy
	// leader-only reads. Empty (the default) means "don't announce" — the
	// node's address stays unknown to the proxy, the documented degraded
	// path. cmd/node.go wires COMMITTED_API_URL to this. See
	// raft-leader-read-proxy.md.
	advertisedAPIURL string

	// diskWatcher configures the background disk-usage watcher (free-space
	// gauges + a soft propose-rejection threshold). An empty Path (the
	// zero value) disables it: the node never gates writes on disk space,
	// the historical behavior. cmd/node.go wires the data dir and the
	// COMMITTED_DISK_*_PERCENT env vars to this. See disk_watcher.go.
	diskWatcher DiskWatcherConfig

	// diskReportInterval is the cadence of the cluster-admission
	// coordinator: how often a member reports its disk state to the leader
	// and the leader recomputes the write-admission verdict. 0 means "use
	// DefaultDiskReportInterval"; negative disables the coordinator
	// entirely (the propose gate then runs on the node-local Phase 1
	// decision alone). cmd/node.go wires COMMITTED_DISK_REPORT_INTERVAL.
	// See disk_cluster.go.
	diskReportInterval time.Duration
	// diskReportClient / diskReportToken configure the HTTP sender that
	// delivers disk reports to the leader's announced API URL: the client
	// carries the TLS trust for self-signed peer APIs (same wiring as the
	// leader-read proxy client) and the token is the cluster's API bearer
	// token. nil/empty work for plaintext, unauthenticated dev clusters.
	diskReportClient *nethttp.Client
	diskReportToken  string
	// diskReportSender, if non-nil, replaces the HTTP sender wholesale.
	// Test-only hook (WithDiskReportSenderForTest) so multi-node admission
	// scenarios can route reports between in-process DBs directly.
	diskReportSender diskReportSender
	// diskTransferCooldown rate-limits disk-pressure leadership transfers.
	// 0 means "use defaultDiskTransferCooldown". Tests shorten it.
	diskTransferCooldown time.Duration
}

// defaultSyncStuckThreshold debounces the "stuck" signal: a syncable must be
// wedged this long before it flags, so normal fast-recovering transients
// never surface as stuck. Tunable later if operators want it shorter.
const defaultSyncStuckThreshold = 30 * time.Second

// DefaultCompactMaxSize is the production default: compact the raft
// log when it grows past 10 GiB. See docs/event-log-architecture.md §
// "Compaction policy".
const DefaultCompactMaxSize uint64 = 10 * 1024 * 1024 * 1024

// DefaultCompactMaxAge is the production default: compact the raft
// log every hour even if it hasn't reached DefaultCompactMaxSize.
const DefaultCompactMaxAge = time.Hour

const DefaultMaxProposalBytes uint64 = 16 * 1024 * 1024

// DefaultScrubInterval is the production cadence at which the leader proposes a
// Scrub when there is unscrubbed RTBF backlog. One hour mirrors the compaction
// age limb and bounds how long deleted PII lingers in the permanent event log
// before physical removal. Operators tune it via COMMITTED_SCRUB_INTERVAL; 0
// disables the automatic scheduler.
const DefaultScrubInterval = time.Hour

// pkgDefaultTransportFactory is the fallback TransportFactory defaultOptions
// uses when a caller doesn't pass WithTransportFactory. It is nil in production
// (the composition root passes WithTransportFactory explicitly); the db test
// suite sets it once via SetTestTransportFactory so its many db.New / NewRaft
// call sites don't each have to wire a transport.
var pkgDefaultTransportFactory TransportFactory

func defaultOptions() options {
	return options{
		tickInterval:       defaultTickInterval,
		logger:             zap.NewNop(),
		compactMaxSize:     DefaultCompactMaxSize,
		compactMaxAge:      DefaultCompactMaxAge,
		maxProposalBytes:   DefaultMaxProposalBytes,
		syncStuckThreshold: defaultSyncStuckThreshold,
		scrubInterval:      DefaultScrubInterval,
		transportFactory:   pkgDefaultTransportFactory,
	}
}

// WithTransportFactory injects the peer-transport constructor. This is the
// inversion-of-control seam that keeps db from importing a concrete transport:
// cmd/node.go passes a factory that builds the HTTP transport, and db drives it
// only through the Transport interface. Required in production — startRaft
// panics without a factory (tests register a default via SetTestTransportFactory).
func WithTransportFactory(f TransportFactory) Option {
	return func(o *options) { o.transportFactory = f }
}

// WithSyncStuckThreshold overrides how long a sync worker must be blocked
// before it flags itself stuck. Tests set it small (tens of ms) so they
// don't wait the 30s production debounce.
func WithSyncStuckThreshold(d time.Duration) Option {
	return func(o *options) { o.syncStuckThreshold = d }
}

// WithTickInterval overrides how often Raft.Tick() is called. Smaller
// intervals make leader election and heartbeats fire faster, which is useful
// in tests where waiting a full second for a single-node election to complete
// is unacceptable. Production callers should leave this at the default.
func WithTickInterval(d time.Duration) Option {
	return func(o *options) { o.tickInterval = d }
}

// WithVersionAnnounce enables the startup feature-level self-announce: the node
// proposes its version.FeatureLevel into the replicated memberVersions map so
// the semantic version-skew gate (db.featureEnabled) can compute the
// cluster-agreed minimum. Production (cmd/node.go) sets this; it is off by
// default so unit tests that assert exact committed-entry counts aren't
// perturbed by the background proposal. A node that never announces reads as
// level 0, which only holds emission — never crashes or diverges.
func WithVersionAnnounce() Option {
	return func(o *options) { o.announceVersion = true }
}

// WithJoin marks this node as joining an existing cluster. A joining node
// bootstraps via raft.RestartNode with an empty state (learning its
// membership from the leader after an AddNode conf change commits) rather
// than raft.StartNode (which would form a competing cluster from the static
// peer set). The peer set is still consumed to seed the transport so the
// node can dial the existing members and bind its own listener. cmd/node.go
// sets this from the COMMITTED_JOIN env var.
func WithJoin() Option {
	return func(o *options) { o.join = true }
}

// WithLeaderChangeGracePeriod overrides how long db.DB's leader-change
// watcher waits before signaling in-flight Propose waiters with
// ErrProposalUnknown after an observed leader transition. The default
// is 3× tickInterval, which gives the new leader a handful of ticks to
// inherit and commit the entry for callers where the apply path still
// wins. Tests set tight values (e.g. 10ms) to exercise the fail-fast
// path deterministically; production callers can leave it at the
// default.
func WithLeaderChangeGracePeriod(d time.Duration) Option {
	return func(o *options) { o.leaderChangeGrace = d }
}

// WithLogger overrides the logger used by DB and its internal Raft instance.
// Defaults to zap.NewNop() so tests run silently.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithMetrics wires Prometheus metrics into DB and its internal Raft
// instance. Defaults to nil (no metrics collected). Pass a *Metrics
// created via metrics.New(registry) to enable instrumentation.
func WithMetrics(m *metrics.Metrics) Option {
	return func(o *options) { o.metrics = m }
}

// WithCompactMaxSize overrides the raft log size (bytes) that
// triggers a compaction. 0 disables the size limb of the compaction
// policy; the age limb may still fire. Tests set tight values
// (e.g. 4096 bytes) to exercise the trigger deterministically.
func WithCompactMaxSize(bytes uint64) Option {
	return func(o *options) { o.compactMaxSize = bytes }
}

// WithCompactMaxAge overrides the maximum wall-clock time between
// compactions. 0 disables the age limb of the compaction policy; the
// size limb may still fire. Tests set tight values (e.g. 10ms) to
// exercise the trigger without ramping raft log size.
func WithCompactMaxAge(d time.Duration) Option {
	return func(o *options) { o.compactMaxAge = d }
}

// WithIngestSupervisorInitialBackoff overrides the first-freeze wait
// duration. Defaults to 100ms. Tests override to sub-millisecond values
// to drive the restart path deterministically.
func WithIngestSupervisorInitialBackoff(d time.Duration) Option {
	return func(o *options) { o.ingestSupervisorInitialBackoff = d }
}

// WithIngestSupervisorMaxBackoff caps the exponential backoff applied
// between consecutive freeze-restart cycles. Defaults to 30s. Tests
// tighten this (e.g. 10ms) to keep the giveup-path scenario fast.
func WithIngestSupervisorMaxBackoff(d time.Duration) Option {
	return func(o *options) { o.ingestSupervisorMaxBackoff = d }
}

// WithIngestSupervisorMaxAttempts sets how many consecutive freezes the
// supervisor will restart through before giving up. Defaults to 20.
// Tests drop this (e.g. 3) so the giveup path completes within a test
// deadline.
func WithIngestSupervisorMaxAttempts(n int) Option {
	return func(o *options) { o.ingestSupervisorMaxAttempts = n }
}

// WithMaxProposalBytes overrides the marshaled-proposal size cap.
// 0 resolves to DefaultMaxProposalBytes.
func WithMaxProposalBytes(bytes uint64) Option {
	return func(o *options) { o.maxProposalBytes = bytes }
}

// WithScrubInterval overrides the automatic-scrub cadence. 0 disables the
// scheduler (the manual POST /v1/scrub lever still works). Tests set tight
// values (tens of ms) to drive the scheduler deterministically; production
// leaves the default (DefaultScrubInterval). cmd/node.go wires
// COMMITTED_SCRUB_INTERVAL to this.
func WithScrubInterval(d time.Duration) Option {
	return func(o *options) { o.scrubInterval = d }
}

// WithAdvertisedAPIURL sets this node's advertised HTTP API base URL (e.g.
// http://n1:8080). On startup the node self-announces it into the replicated
// memberAPIURLs map so other nodes — in particular a follower proxying a
// leader-only read (GET /v1/membership) — can resolve this node's API
// address. Empty (the default) disables the announce: the node's address
// stays unknown to the proxy and a leader-only read on a follower degrades to
// 503 + leaderId. cmd/node.go wires COMMITTED_API_URL to this. See
// raft-leader-read-proxy.md.
func WithAdvertisedAPIURL(url string) Option {
	return func(o *options) { o.advertisedAPIURL = url }
}

// WithDiskWatcher enables the background disk-usage watcher. cfg.Path is the
// data directory to statfs; an empty Path leaves the watcher disabled (the
// node never gates writes on disk space). Zero-valued threshold/interval
// fields resolve to their Default* values. cmd/node.go builds the config from
// the data dir and the COMMITTED_DISK_*_PERCENT env vars. See disk_watcher.go.
func WithDiskWatcher(cfg DiskWatcherConfig) Option {
	return func(o *options) { o.diskWatcher = cfg }
}

// WithDiskReportInterval sets the cadence of the cluster-admission
// coordinator: how often a member reports its disk state to the leader and
// the leader recomputes the cluster write-admission verdict. d <= 0 disables
// the coordinator entirely, leaving the propose gate on the node-local
// (Phase 1) decision alone. Default DefaultDiskReportInterval. cmd/node.go
// wires COMMITTED_DISK_REPORT_INTERVAL. See disk_cluster.go.
func WithDiskReportInterval(d time.Duration) Option {
	return func(o *options) {
		if d <= 0 {
			d = -1
		}
		o.diskReportInterval = d
	}
}

// WithDiskReportHTTP configures the HTTP sender that delivers disk reports
// to the leader's announced API URL. client carries the TLS trust for
// self-signed peer APIs (cmd/node.go passes the same client it builds for
// the leader-read proxy; nil gets a timeout-bounded system-root default) and
// token is the cluster's API bearer token (reports hit the authenticated
// POST /v1/node/disk-report endpoint, so every node must share the token —
// already the deployment shape, since the leader-read proxy forwards client
// tokens between nodes).
func WithDiskReportHTTP(client *nethttp.Client, token string) Option {
	return func(o *options) {
		o.diskReportClient = client
		o.diskReportToken = token
	}
}

// WithTLSInfo enables mTLS on the raft peer transport. Pass a
// transport.TLSInfo populated with CertFile, KeyFile, and TrustedCAFile
// to require CA-signed client certs on inbound peer connections and to
// present this node's cert on outbound ones. Peer URLs must use the
// `https://` scheme for TLS dialing; mixed schemes across peers are a
// configuration error and will manifest as handshake failures at
// connection time. Default (no call) leaves the transport as plaintext
// HTTP — the historical behavior.
func WithTLSInfo(info *transport.TLSInfo) Option {
	return func(o *options) { o.tlsInfo = info }
}

// WithAPIToken sets the cluster bearer token the peer transport sends on its
// requests. The composition root (cmd/node.go) reads COMMITTED_API_TOKEN once
// and injects it here, so the transport constructor doesn't reach into the
// process environment itself. Default (no call, or "") leaves peer requests
// unauthenticated.
func WithAPIToken(token string) Option {
	return func(o *options) { o.apiToken = token }
}
