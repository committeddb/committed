package db

import (
	"time"

	"github.com/philborlin/committed/internal/cluster/metrics"
	"go.uber.org/zap"
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
	logger       *zap.Logger
	metrics      *metrics.Metrics
	// transportWrapper, if non-nil, is applied to the Transport that
	// startRaft constructs via httptransport.New, and the returned value
	// is used in place of the original. Test-only hook set via
	// WithTransportWrapperForTest (export_test.go) so the adversarial
	// suite can inject fault-injection wrappers around the real transport
	// without forking a parallel constructor path. Production callers
	// leave this nil and get the plain HttpTransport.
	transportWrapper func(Transport) Transport
}

func defaultOptions() options {
	return options{
		tickInterval: defaultTickInterval,
		logger:       zap.NewNop(),
	}
}

// WithTickInterval overrides how often Raft.Tick() is called. Smaller
// intervals make leader election and heartbeats fire faster, which is useful
// in tests where waiting a full second for a single-node election to complete
// is unacceptable. Production callers should leave this at the default.
func WithTickInterval(d time.Duration) Option {
	return func(o *options) { o.tickInterval = d }
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
