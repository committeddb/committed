package db

import (
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster/metrics"
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

	// ingestSupervisor{InitialBackoff,MaxBackoff,MaxAttempts,HealthyWindow}
	// govern the auto-restart behavior when an ingest worker parks in the
	// ErrProposalUnknown freeze branch. The supervisor waits backoff (starting
	// at InitialBackoff, doubling on each successive freeze, capped at
	// MaxBackoff) before re-registering the ingestable via db.Ingest. After
	// MaxAttempts consecutive freezes within HealthyWindow the supervisor
	// gives up and emits IngestSupervisorGiveup; any freeze-free run longer
	// than HealthyWindow resets the counter so a fresh flap starts clean.
	// Zero values mean "use package defaults" (see default* constants in
	// db.go).
	ingestSupervisorInitialBackoff time.Duration
	ingestSupervisorMaxBackoff     time.Duration
	ingestSupervisorMaxAttempts    int
	ingestSupervisorHealthyWindow  time.Duration

	// ingestFreezeDrainTimeout bounds how long the ingest worker
	// waits for its in-flight position-bump acks to resolve before
	// returning ingestExitFreeze. A longer budget makes the
	// supervisor's subsequent storage.Position read more accurate
	// (fewer unresolved bumps means fewer rows re-read on replay);
	// a shorter budget caps the MTTR for the supervisor's restart.
	// 0 resolves to 2 * leaderChangeGrace at New time — the rough
	// window within which the leader-change watcher will either
	// apply-ack or ErrProposalUnknown-signal any in-flight waiter.
	ingestFreezeDrainTimeout time.Duration

	maxProposalBytes uint64
}

// DefaultCompactMaxSize is the production default: compact the raft
// log when it grows past 10 GiB. See docs/event-log-architecture.md §
// "Compaction policy".
const DefaultCompactMaxSize uint64 = 10 * 1024 * 1024 * 1024

// DefaultCompactMaxAge is the production default: compact the raft
// log every hour even if it hasn't reached DefaultCompactMaxSize.
const DefaultCompactMaxAge = time.Hour

const DefaultMaxProposalBytes uint64 = 16 * 1024 * 1024

func defaultOptions() options {
	return options{
		tickInterval:     defaultTickInterval,
		logger:           zap.NewNop(),
		compactMaxSize:   DefaultCompactMaxSize,
		compactMaxAge:    DefaultCompactMaxAge,
		maxProposalBytes: DefaultMaxProposalBytes,
	}
}

// WithTickInterval overrides how often Raft.Tick() is called. Smaller
// intervals make leader election and heartbeats fire faster, which is useful
// in tests where waiting a full second for a single-node election to complete
// is unacceptable. Production callers should leave this at the default.
func WithTickInterval(d time.Duration) Option {
	return func(o *options) { o.tickInterval = d }
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

// WithIngestSupervisorHealthyWindow controls how long an ingestable has
// to run freeze-free before the supervisor considers the previous flap
// sequence "resolved" and resets the consecutive-freeze counter.
// Defaults to 60s.
func WithIngestSupervisorHealthyWindow(d time.Duration) Option {
	return func(o *options) { o.ingestSupervisorHealthyWindow = d }
}

// WithIngestFreezeDrainTimeout caps how long the ingest worker waits
// on its in-flight position-bump acks before returning from the
// freeze branch. Tests tighten this to exercise the timeout path.
// Zero leaves the default (2 * leaderChangeGrace) in place.
func WithIngestFreezeDrainTimeout(d time.Duration) Option {
	return func(o *options) { o.ingestFreezeDrainTimeout = d }
}

// WithMaxProposalBytes overrides the marshaled-proposal size cap.
// 0 resolves to DefaultMaxProposalBytes.
func WithMaxProposalBytes(bytes uint64) Option {
	return func(o *options) { o.maxProposalBytes = bytes }
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
