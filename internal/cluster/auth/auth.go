// Package auth validates the node's shared or separated bearer credentials.
package auth

import (
	"fmt"
	"strings"
)

// Tokens is a validated credential set. Its zero value is unauthenticated
// development mode. Fields are private so partial split configurations cannot
// be constructed by callers.
type Tokens struct {
	api, membership, peer string
}

// NewTokens accepts no credentials, an API token alone (legacy shared access),
// or three distinct tokens (split access). Errors never include token values.
func NewTokens(api, membership, peer string) (Tokens, error) {
	api, membership, peer = strings.TrimSpace(api), strings.TrimSpace(membership), strings.TrimSpace(peer)
	if membership == "" && peer == "" {
		return Tokens{api: api}, nil
	}
	if api == "" || membership == "" || peer == "" {
		return Tokens{}, fmt.Errorf("COMMITTED_API_TOKEN, COMMITTED_MEMBERSHIP_TOKEN and COMMITTED_PEER_TOKEN must all be nonempty for split authorization")
	}
	if api == membership || api == peer || membership == peer {
		return Tokens{}, fmt.Errorf("COMMITTED_API_TOKEN, COMMITTED_MEMBERSHIP_TOKEN and COMMITTED_PEER_TOKEN must be distinct")
	}
	return Tokens{api: api, membership: membership, peer: peer}, nil
}

func (t Tokens) Split() bool { return t.membership != "" }
func (t Tokens) API() string { return t.api }
func (t Tokens) Membership() string {
	if !t.Split() {
		return t.api
	}
	return t.membership
}

func (t Tokens) Peer() string {
	if !t.Split() {
		return t.api
	}
	return t.peer
}
