package http

import (
	httpgo "net/http"

	"github.com/philborlin/committed/internal/version"
)

// Version returns the running binary's build metadata as JSON. It is
// unauthenticated and state-free — same category as /health — so
// operators can quickly answer "are all nodes on the same build?"
// without needing a token or a leader.
func (h *HTTP) Version(w httpgo.ResponseWriter, r *httpgo.Request) {
	writeJSONStatus(w, httpgo.StatusOK, version.Get())
}
