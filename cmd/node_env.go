package cmd

import (
	"fmt"
	"log"
	"maps"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
)

// getenvDefault returns the value of env var name, or def when it is
// unset or empty.
func getenvDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// nodeID reads COMMITTED_NODE_ID and fatal-exits on a bad value. The
// parsing lives in parseNodeID so it can be unit-tested without the
// process-exiting wrapper.
func nodeID() uint64 {
	id, err := parseNodeID(os.Getenv("COMMITTED_NODE_ID"))
	if err != nil {
		// G706 false positive: the value is an operator-supplied env var.
		log.Fatalf("%v", err) //nolint:gosec // G706
	}
	return id
}

// parseNodeID interprets the COMMITTED_NODE_ID env value: empty defaults
// to 1, otherwise it must be a positive uint64. A zero or unparseable
// value is an error rather than a silent fallback — collapsing a
// mistyped identity onto ID 1 would let two nodes claim the same raft ID
// and corrupt the group, which is far worse than refusing to start.
func parseNodeID(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1, nil
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("COMMITTED_NODE_ID must be a positive integer (got %q)", raw)
	}
	return id, nil
}

// loadPeers builds the static raft peer set used for first-boot
// bootstrap and fatal-exits on a malformed COMMITTED_PEERS. Parsing
// lives in parsePeers so it can be unit-tested without the wrapper.
func loadPeers(id uint64) db.Peers {
	peers, err := parsePeers(id, os.Getenv("COMMITTED_PEERS"), getenvDefault("COMMITTED_PEER_URL", "http://127.0.0.1:9022"))
	if err != nil {
		// G706 false positive: the value is an operator-supplied env var.
		log.Fatalf("%v", err) //nolint:gosec // G706
	}
	return peers
}

// parsePeers builds the static raft peer set for first-boot bootstrap
// (raft.StartNode).
//
// raw is the COMMITTED_PEERS env value: when non-empty it is the full
// cluster membership as a comma-separated list of id=url pairs, e.g.
//
//	COMMITTED_PEERS="1=http://n1:9022,2=http://n2:9022,3=http://n3:9022"
//
// Every node receives the same COMMITTED_PEERS and the set must include
// this node's own id. Membership is consumed only on first boot; on
// restart it is restored from the WAL (raft.RestartNode), so editing
// COMMITTED_PEERS after a node has state has no effect — use the
// "committed member add/remove" commands (the /v1/membership API) for
// live membership changes. A node joining an existing cluster sets
// COMMITTED_JOIN=true so its COMMITTED_PEERS seeds the transport without
// bootstrapping a competing configuration. See
// docs/operations/membership.md.
//
// When raw is empty the node bootstraps a single-node cluster
// advertising selfURL (COMMITTED_PEER_URL) for itself — the historical
// laptop-dev default.
//
// Malformed input is an error rather than best-effort: a bad peer set
// yields split-brain or a node that can never reach quorum, both worse
// than a loud refusal to start.
func parsePeers(id uint64, raw, selfURL string) (db.Peers, error) {
	if raw == "" {
		return db.Peers{id: selfURL}, nil
	}

	peers := make(db.Peers)
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		k, v, ok := strings.Cut(entry, "=")
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("COMMITTED_PEERS entry %q is not in id=url form", entry)
		}
		pid, err := strconv.ParseUint(k, 10, 64)
		if err != nil || pid == 0 {
			return nil, fmt.Errorf("COMMITTED_PEERS entry %q has an invalid peer id", entry)
		}
		if _, dup := peers[pid]; dup {
			return nil, fmt.Errorf("COMMITTED_PEERS has a duplicate peer id %d", pid)
		}
		peers[pid] = v
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("COMMITTED_PEERS is set but contains no valid peers")
	}
	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("COMMITTED_PEERS must include this node's own COMMITTED_NODE_ID (%d)", id)
	}
	return peers, nil
}

// parseInt64Env reads an int64-valued env var. Returns (0, false)
// when the var is unset or unparseable, with a logged warning for
// the unparseable case — a typo in an HTTP-limit env var should be
// visible, not silently reverted to the default.
func parseInt64Env(name string) (int64, bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || v <= 0 {
		zap.L().Warn(name+" invalid, using default", zap.String("value", raw))
		return 0, false
	}
	return v, true
}

// loadCORSOrigins parses the comma-separated COMMITTED_HTTP_CORS_ORIGINS
// allowlist. Unset or empty returns (nil, nil) — CORS stays off and the
// http package emits no Access-Control-* headers. Each entry must be the
// literal "*" (allow any origin) or an absolute scheme://host origin;
// anything else is a hard error so a typo'd origin fails fast at startup
// rather than silently rejecting every browser preflight at runtime.
//
// Returning (origins, err) instead of calling log.Fatalf lets node_test.go
// exercise the parsing and error cases directly, matching loadAPITLSConfig.
func loadCORSOrigins() ([]string, error) {
	raw := os.Getenv("COMMITTED_HTTP_CORS_ORIGINS")
	if raw == "" {
		return nil, nil
	}

	var origins []string
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if entry == "*" {
			origins = append(origins, entry)
			continue
		}
		u, err := url.Parse(entry)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("COMMITTED_HTTP_CORS_ORIGINS entry %q is not a valid origin (want scheme://host, e.g. https://app.example.com, or \"*\")", entry)
		}
		origins = append(origins, entry)
	}
	if len(origins) == 0 {
		return nil, fmt.Errorf("COMMITTED_HTTP_CORS_ORIGINS is set but contains no valid origins")
	}
	return origins, nil
}

// parseListEnv reads a comma-separated env var into a trimmed, non-empty
// string slice. Unset or empty returns nil so the caller falls back to
// its own defaults.
func parseListEnv(name string) []string {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}
	var out []string
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry != "" {
			out = append(out, entry)
		}
	}
	return out
}

// apiTokenEnv is the one read of COMMITTED_API_TOKEN, shared by the node
// (server side of the compare, and the disk-report sender) and the member
// CLI (client side). Surrounding whitespace is trimmed like every other
// setting: a secret materialised from a file carries a trailing newline
// more often than not, and untrimmed it is unusable from EVERY direction —
// the server compares against "token\n", committed's own sender is refused
// by net/http for a control character in the header, and a curl client
// sends the shell-stripped "token" and gets 401. A token that deliberately
// ends in whitespace is not a real case.
func apiTokenEnv() string {
	return strings.TrimSpace(os.Getenv("COMMITTED_API_TOKEN"))
}

// boolEnv reports whether env var name holds a truthy value, parsed by
// strconv.ParseBool ("1", "t", "true", "TRUE", etc.). Unset or empty reads
// as false — a flag-style env var is opt-in.
//
// A NON-EMPTY value that does not parse is an ERROR, never a silent false.
// ParseBool rejects "yes", "on", "y" and "enabled", all common in deployment
// tooling, and reading those as false is how a COMMITTED_JOIN=yes node
// bootstraps its own raft configuration and split-brains against the cluster
// it meant to join — the exact failure the flag exists to prevent. This
// helper reports the error so a caller that must hand it on can (see
// boolEnvOrExit for the node's own startup reads, which refuse to boot).
func boolEnv(name string) (bool, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s=%q is not a boolean: use true or false (also accepted: 1/0, t/f)", name, raw)
	}
	return v, nil
}

// boolEnvOrExit is boolEnv for the node command's own startup reads, where
// there is no caller to hand an error to and continuing would mean running
// without a setting the operator asked for. It stays separate from boolEnv
// so a helper that must REPORT rather than exit — loadProxyClient documents
// exactly that, so node_test.go can drive its failure cases — never inherits
// a process exit from a shared parser.
func boolEnvOrExit(name string) bool {
	v, err := boolEnv(name)
	if err != nil {
		log.Fatalf("%v — refusing to start rather than silently treating it as false", err) //nolint:gosec // G706: the value is the operator's own env
	}
	return v
}

// parsePercentEnv reads a free-space percent threshold (0,100) for the disk
// watcher. Unset, empty, or out-of-range returns 0, which DiskWatcherConfig
// resolves to the matching db.DefaultDisk*Percent — so a typo'd value warns
// and falls back rather than silently disabling a guardrail.
func parsePercentEnv(name string) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || v <= 0 || v >= 100 {
		zap.L().Warn(name+" invalid (want a percent between 0 and 100), using default", zap.String("value", raw))
		return 0
	}
	return v
}

// parseDurationEnv reads a Go-duration setting that must be POSITIVE — a
// timeout or cadence where zero is meaningless. Unset says nothing; zero,
// negative, or unparseable warns and the caller keeps its default.
func parseDurationEnv(name string) (time.Duration, bool) {
	return parseDurationEnvWithZero(name, false)
}

// parseDisableableDurationEnv reads a Go-duration setting where zero means
// DISABLE rather than "use the default" — a switch, not a timeout. It accepts
// every spelling of zero the syntax allows ("0", "0s", "0ms"), not just the
// literal character: the help text teaches duration syntax, so an operator
// writing "0s" meant to disable and must not silently get the default. That
// gap left COMMITTED_SCRUB_INTERVAL=0 unable to stop the erasure scrubber.
func parseDisableableDurationEnv(name string) (time.Duration, bool) {
	return parseDurationEnvWithZero(name, true)
}

// parseDurationEnvWithZero is the shared body of the two above: one place
// that trims, parses, and reports, so the two intents cannot drift in how
// they treat whitespace or a bad value.
func parseDurationEnvWithZero(name string, zeroDisables bool) (time.Duration, bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false
	}
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	switch {
	case err != nil:
		zap.L().Warn(name+" invalid, using default", zap.String("value", raw))
		return 0, false
	case v < 0 && zeroDisables:
		zap.L().Warn(name+" must not be negative (zero disables), using default", zap.String("value", raw))
		return 0, false
	case v <= 0 && !zeroDisables:
		zap.L().Warn(name+" must be a positive duration, using default", zap.String("value", raw))
		return 0, false
	}
	return v, true
}

// shutdownTimeout returns the configured graceful-shutdown deadline.
// Reads COMMITTED_SHUTDOWN_TIMEOUT (Go duration syntax, e.g. "45s").
// An unset or unparseable value falls back to defaultShutdownTimeout
// with a warning — a misconfigured env var should not silently disable
// the graceful path.
func shutdownTimeout() time.Duration {
	// Through the shared parser: a third hand-rolled copy is how the three
	// duration settings came to disagree about whitespace and about what a
	// bad value says.
	if d, ok := parseDurationEnv("COMMITTED_SHUTDOWN_TIMEOUT"); ok {
		return d
	}
	return defaultShutdownTimeout
}

// removedEnvVars is the environment's counterpart to the config vocabulary's
// removed-spelling ledger: a setting 0.8.0 renamed, mapped to what it became.
// Silence is not an option for these. Peer mTLS is configured all-or-nothing
// and an unset trio means PLAINTEXT, so a deployment that still sets the old
// names would come up with peer TLS quietly off — a security downgrade
// delivered by an upgrade. Fail loudly instead, naming the new spelling.
var removedEnvVars = map[string]string{
	"COMMITTED_TLS_CA_FILE":                   "COMMITTED_PEER_TLS_CA_FILE",
	"COMMITTED_TLS_CERT_FILE":                 "COMMITTED_PEER_TLS_CERT_FILE",
	"COMMITTED_TLS_KEY_FILE":                  "COMMITTED_PEER_TLS_KEY_FILE",
	"COMMITTED_HTTP_TLS_CA_FILE":              "COMMITTED_HTTP_CLIENT_TLS_CA_FILE",
	"COMMITTED_HTTP_TLS_INSECURE_SKIP_VERIFY": "COMMITTED_HTTP_CLIENT_TLS_INSECURE_SKIP_VERIFY",
}

// checkRemovedEnvVars reports every removed setting the environment still
// sets, as one message naming each replacement — an operator fixes the whole
// deployment in one pass rather than one restart per variable.
func checkRemovedEnvVars() error {
	var found []string
	for _, old := range slices.Sorted(maps.Keys(removedEnvVars)) {
		if os.Getenv(old) != "" {
			found = append(found, old+" is now "+removedEnvVars[old])
		}
	}
	if len(found) == 0 {
		return nil
	}
	return fmt.Errorf("these COMMITTED_* settings were renamed in 0.8.0 and are no longer read: %s — rename them in your deployment and restart (peer TLS is all-or-nothing, so an unrenamed trio would silently run plaintext)",
		strings.Join(found, "; "))
}
