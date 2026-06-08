package cmd

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// memberRequestTimeout bounds the whole add/remove call. A joint-consensus
// membership change is two committed entries (enter joint, then auto-leave),
// so it normally completes in a few raft round-trips; 30s is a generous
// ceiling that still fails fast if the target node can't reach a quorum.
const memberRequestTimeout = 30 * time.Second

var (
	memberTarget   string
	memberID       uint64
	memberURL      string
	memberLearner  bool
	memberToken    string
	memberInsecure bool
)

var memberCmd = &cobra.Command{
	Use:   "member",
	Short: "Manage live raft cluster membership",
	Long: `Add or remove nodes from a running committed cluster using
joint-consensus (ConfChangeV2) raft reconfiguration.

The command POSTs/DELETEs the target node's authenticated /v1/membership
endpoint. The target defaults to this host's API address (COMMITTED_API_ADDR);
point --target at any cluster member to drive the change remotely — a
follower forwards the proposal to the leader.

Authentication and TLS mirror the node's API configuration:

  COMMITTED_API_TOKEN           bearer token sent as Authorization (or --token)
  COMMITTED_HTTP_TLS_CERT_FILE  when set (and --target is not given), the
                                local API is HTTPS, so the call uses https://

Adding a node is a two-step operator workflow: first start the new node in
join mode (COMMITTED_JOIN=true) with a peer set that lets it reach the
existing members, then run "member add" naming its id and advertised peer
URL. See docs/operations/membership.md.`,
}

var memberAddCmd = &cobra.Command{
	Use:          "add",
	Short:        "Add a node to the cluster (a voter, or a learner with --learner)",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if memberID == 0 {
			return fmt.Errorf("--id is required and must be a positive integer")
		}
		if memberURL == "" {
			return fmt.Errorf("--url is required (the new node's advertised peer URL)")
		}
		body, err := json.Marshal(map[string]any{"id": memberID, "url": memberURL, "learner": memberLearner})
		if err != nil {
			return err
		}
		return memberDo(nethttp.MethodPost, "/v1/membership", body)
	},
}

var memberPromoteCmd = &cobra.Command{
	Use:          "promote",
	Short:        "Promote a learner to a voter",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if memberID == 0 {
			return fmt.Errorf("--id is required and must be a positive integer")
		}
		return memberDo(nethttp.MethodPost, fmt.Sprintf("/v1/membership/%d/promote", memberID), nil)
	},
}

var memberRemoveCmd = &cobra.Command{
	Use:          "remove",
	Short:        "Remove a node from the cluster",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if memberID == 0 {
			return fmt.Errorf("--id is required and must be a positive integer")
		}
		return memberDo(nethttp.MethodDelete, fmt.Sprintf("/v1/membership/%d", memberID), nil)
	},
}

// memberDo performs the membership HTTP request against the resolved target
// and maps the response to an error (non-2xx) or nil (2xx). The endpoint
// returns 204 on success; it blocks server-side until the change has taken
// effect, so a 204 here means the cluster has applied the new membership.
func memberDo(method, path string, body []byte) error {
	base, err := memberBaseURL()
	if err != nil {
		return err
	}
	url := base + path

	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := nethttp.NewRequest(method, url, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token := memberAPIToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client, err := memberClient()
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("member: %s %s: %w", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Printf("membership change applied (%s %s)\n", method, url)
		return nil
	}

	// Surface the server's structured error body — it carries a code and a
	// human-readable message (see internal/cluster/http/errors.go).
	msg, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("member: %s %s returned %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(msg)))
}

// memberBaseURL resolves the scheme://host:port to hit. An explicit
// --target wins; otherwise it is derived from COMMITTED_API_ADDR the same
// way the healthcheck probe derives its loopback URL (empty/wildcard host →
// 127.0.0.1, https when the local API serves TLS).
func memberBaseURL() (string, error) {
	if memberTarget != "" {
		return strings.TrimRight(memberTarget, "/"), nil
	}

	addr := getenvDefault("COMMITTED_API_ADDR", ":8080")
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host, port = "", addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	scheme := "http"
	if os.Getenv("COMMITTED_HTTP_TLS_CERT_FILE") != "" {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, port)), nil
}

// memberAPIToken returns the bearer token to authenticate with: --token if
// given, else COMMITTED_API_TOKEN, else empty (the API runs unauthenticated).
func memberAPIToken() string {
	if memberToken != "" {
		return memberToken
	}
	return os.Getenv("COMMITTED_API_TOKEN")
}

// memberClient builds the HTTP client. For an https target it enables TLS;
// --insecure skips server-certificate verification, which an operator needs
// when the cluster uses self-signed certs or when targeting a node by an
// address its cert doesn't list.
func memberClient() (*nethttp.Client, error) {
	c := &nethttp.Client{Timeout: memberRequestTimeout}
	if memberInsecure {
		c.Transport = &nethttp.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // G402: operator opt-in via --insecure
				MinVersion:         tls.VersionTLS12,
			},
		}
	}
	return c, nil
}

func init() {
	memberCmd.PersistentFlags().StringVar(&memberTarget, "target", "", "base URL of a cluster node's API (default: local COMMITTED_API_ADDR)")
	memberCmd.PersistentFlags().StringVar(&memberToken, "token", "", "API bearer token (default: COMMITTED_API_TOKEN)")
	memberCmd.PersistentFlags().BoolVar(&memberInsecure, "insecure", false, "skip TLS certificate verification when the target is https")

	memberAddCmd.Flags().Uint64Var(&memberID, "id", 0, "raft node id of the member to add")
	memberAddCmd.Flags().StringVar(&memberURL, "url", "", "advertised peer URL of the member to add (e.g. http://n4:9022)")
	memberAddCmd.Flags().BoolVar(&memberLearner, "learner", false, "add as a non-voting learner instead of a voter (promote later with 'member promote')")

	memberRemoveCmd.Flags().Uint64Var(&memberID, "id", 0, "raft node id of the member to remove")

	memberPromoteCmd.Flags().Uint64Var(&memberID, "id", 0, "raft node id of the learner to promote to a voter")

	memberCmd.AddCommand(memberAddCmd)
	memberCmd.AddCommand(memberRemoveCmd)
	memberCmd.AddCommand(memberPromoteCmd)
	rootCmd.AddCommand(memberCmd)
}
